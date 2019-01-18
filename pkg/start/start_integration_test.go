package start

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/openshift/cluster-version-operator/pkg/cvo"

	"k8s.io/apimachinery/pkg/util/diff"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	randutil "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	configv1 "github.com/openshift/api/config/v1"
	clientset "github.com/openshift/client-go/config/clientset/versioned"
	"github.com/openshift/cluster-version-operator/lib/resourcemerge"
)

var (
	version_0_0_1 = map[string]interface{}{
		"release-manifests": map[string]interface{}{
			"image-references": `
			{
				"kind": "ImageStream",
				"apiVersion": "image.openshift.io/v1",
				"metadata": {
					"name": "0.0.1"
				}
			}
			`,
			// this manifest should not have ReleaseImage replaced because it is part of the user facing payload
			"config2.json": `
			{
				"kind": "ConfigMap",
				"apiVersion": "v1",
				"metadata": {
					"name": "config2",
					"namespace": "$(NAMESPACE)"
				},
				"data": {
					"version": "0.0.1",
					"releaseImage": "{{.ReleaseImage}}"
				}
			}
			`,
		},
		"manifests": map[string]interface{}{
			// this manifest is part of the innate payload and should have ReleaseImage replaced
			"config1.json": `
			{
				"kind": "ConfigMap",
				"apiVersion": "v1",
				"metadata": {
					"name": "config1",
					"namespace": "$(NAMESPACE)"
				},
				"data": {
					"version": "0.0.1",
					"releaseImage": "{{.ReleaseImage}}"
				}
			}
			`,
		},
	}
	version_0_0_2 = map[string]interface{}{
		"release-manifests": map[string]interface{}{
			"image-references": `
			{
				"kind": "ImageStream",
				"apiVersion": "image.openshift.io/v1",
				"metadata": {
					"name": "0.0.2"
				}
			}
			`,
			"config2.json": `
			{
				"kind": "ConfigMap",
				"apiVersion": "v1",
				"metadata": {
					"name": "config2",
					"namespace": "$(NAMESPACE)"
				},
				"data": {
					"version": "0.0.2",
					"releaseImage": "{{.ReleaseImage}}"
				}
			}
			`,
		},
		"manifests": map[string]interface{}{
			"config1.json": `
			{
				"kind": "ConfigMap",
				"apiVersion": "v1",
				"metadata": {
					"name": "config1",
					"namespace": "$(NAMESPACE)"
				},
				"data": {
					"version": "0.0.2",
					"releaseImage": "{{.ReleaseImage}}"
				}
			}
			`,
		},
	}
	version_0_0_2_failing = map[string]interface{}{
		"release-manifests": map[string]interface{}{
			"image-references": `
			{
				"kind": "ImageStream",
				"apiVersion": "image.openshift.io/v1",
				"metadata": {
					"name": "0.0.2"
				}
			}
			`,
			// has invalid label value, API server will reject
			"config2.json": `
			{
				"kind": "ConfigMap",
				"apiVersion": "v1",
				"metadata": {
					"name": "config2",
					"namespace": "$(NAMESPACE)",
					"labels": {"": ""}
				},
				"data": {
					"version": "0.0.2",
					"releaseImage": "{{.ReleaseImage}}"
				}
			}
			`,
		},
		"manifests": map[string]interface{}{
			"config1.json": `
			{
				"kind": "ConfigMap",
				"apiVersion": "v1",
				"metadata": {
					"name": "config1",
					"namespace": "$(NAMESPACE)"
				},
				"data": {
					"version": "0.0.2",
					"releaseImage": "{{.ReleaseImage}}"
				}
			}
			`,
		},
	}
)

func TestIntegrationCVO_initializeAndUpgrade(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") != "1" {
		t.Skipf("Integration tests are disabled unless TEST_INTEGRATION=1")
	}
	t.Parallel()

	// use the same client setup as the start command
	cb, err := newClientBuilder("")
	if err != nil {
		t.Fatal(err)
	}
	cfg := cb.RestConfig()
	kc := cb.KubeClientOrDie("integration-test")
	client := cb.ClientOrDie("integration-test")

	ns := fmt.Sprintf("e2e-cvo-%s", randutil.String(4))

	if _, err := kc.Core().Namespaces().Create(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Config().ClusterVersions().Delete(ns, nil); err != nil {
			t.Logf("failed to delete cluster version %s: %v", ns, err)
		}
		if err := kc.Core().Namespaces().Delete(ns, nil); err != nil {
			t.Logf("failed to delete namespace %s: %v", ns, err)
		}
	}()

	dir, err := ioutil.TempDir("", "cvo-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := createContent(filepath.Join(dir, "0.0.1"), version_0_0_1, map[string]string{"NAMESPACE": ns}); err != nil {
		t.Fatal(err)
	}
	if err := createContent(filepath.Join(dir, "0.0.2"), version_0_0_2, map[string]string{"NAMESPACE": ns}); err != nil {
		t.Fatal(err)
	}
	payloadImage1 := "arbitrary/release:image"
	payloadImage2 := "arbitrary/release:image-2"
	retriever := &mapPayloadRetriever{map[string]string{
		payloadImage1: filepath.Join(dir, "0.0.1"),
		payloadImage2: filepath.Join(dir, "0.0.2"),
	}}

	options := NewOptions()
	options.Namespace = ns
	options.Name = ns
	options.ListenAddr = ""
	options.NodeName = "test-node"
	options.ReleaseImage = payloadImage1
	options.PayloadOverride = filepath.Join(dir, "ignored")
	options.EnableMetrics = false
	options.ResyncInterval = 5 * time.Second
	controllers := options.NewControllerContext(cb)

	worker := cvo.NewSyncWorker(retriever, cvo.NewResourceBuilder(cfg), 5*time.Second, wait.Backoff{Steps: 3}).(*cvo.SyncWorker)
	controllers.CVO.SetSyncWorkerForTesting(worker)

	stopCh := make(chan struct{})
	defer close(stopCh)
	controllers.Start(stopCh)

	t.Logf("wait until we observe the cluster version become available")
	lastCV, err := waitForAvailableUpdate(t, client, ns, false, "0.0.1")
	if err != nil {
		t.Logf("latest version:\n%s", printCV(lastCV))
		t.Fatalf("cluster version never became available: %v", err)
	}

	status := worker.Status()

	t.Logf("verify the available cluster version's status matches our expectations")
	t.Logf("Cluster version:\n%s", printCV(lastCV))
	verifyClusterVersionStatus(t, lastCV, configv1.Update{Payload: payloadImage1, Version: "0.0.1"}, 1)
	verifyReleasePayload(t, kc, ns, "0.0.1", payloadImage1)

	t.Logf("wait for the next resync and verify that status didn't change")
	if err := wait.Poll(time.Second, 30*time.Second, func() (bool, error) {
		updated := worker.Status()
		if updated.Completed > status.Completed {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	cv, err := client.Config().ClusterVersions().Get(ns, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cv.Status, lastCV.Status) {
		t.Fatalf("unexpected: %s", diff.ObjectReflectDiff(lastCV.Status, cv.Status))
	}
	verifyReleasePayload(t, kc, ns, "0.0.1", payloadImage1)

	t.Logf("trigger an update to a new version")
	cv, err = client.Config().ClusterVersions().Patch(ns, types.MergePatchType, []byte(fmt.Sprintf(`{"spec":{"desiredUpdate":{"payload":"%s"}}}`, payloadImage2)))
	if err != nil {
		t.Fatal(err)
	}
	if cv.Spec.DesiredUpdate == nil {
		t.Fatalf("cluster desired version was not preserved: %s", printCV(cv))
	}

	t.Logf("wait for the new version to be available")
	lastCV, err = waitForAvailableUpdate(t, client, ns, false, "0.0.1", "0.0.2")
	if err != nil {
		t.Logf("latest version:\n%s", printCV(lastCV))
		t.Fatalf("cluster version never reached available at 0.0.2: %v", err)
	}
	t.Logf("Upgraded version:\n%s", printCV(lastCV))
	verifyClusterVersionStatus(t, lastCV, configv1.Update{Payload: payloadImage2, Version: "0.0.2"}, 2)
	verifyReleasePayload(t, kc, ns, "0.0.2", payloadImage2)

	t.Logf("delete an object so that the next resync will recover it")
	if err := kc.CoreV1().ConfigMaps(ns).Delete("config1", nil); err != nil {
		t.Fatalf("couldn't delete CVO managed object: %v", err)
	}

	status = worker.Status()

	t.Logf("wait for the next resync and verify that status didn't change")
	if err := wait.Poll(time.Second, 30*time.Second, func() (bool, error) {
		updated := worker.Status()
		if updated.Completed > status.Completed {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	cv, err = client.Config().ClusterVersions().Get(ns, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cv.Status, lastCV.Status) {
		t.Fatalf("unexpected: %s", diff.ObjectReflectDiff(lastCV.Status, cv.Status))
	}

	// should have recreated our deleted object
	verifyReleasePayload(t, kc, ns, "0.0.2", payloadImage2)
}

func TestIntegrationCVO_initializeAndHandleError(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") != "1" {
		t.Skipf("Integration tests are disabled unless TEST_INTEGRATION=1")
	}
	t.Parallel()

	// use the same client setup as the start command
	cb, err := newClientBuilder("")
	if err != nil {
		t.Fatal(err)
	}
	cfg := cb.RestConfig()
	kc := cb.KubeClientOrDie("integration-test")
	client := cb.ClientOrDie("integration-test")

	ns := fmt.Sprintf("e2e-cvo-%s", randutil.String(4))

	if _, err := kc.Core().Namespaces().Create(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Config().ClusterVersions().Delete(ns, nil); err != nil {
			t.Logf("failed to delete cluster version %s: %v", ns, err)
		}
		if err := kc.Core().Namespaces().Delete(ns, nil); err != nil {
			t.Logf("failed to delete namespace %s: %v", ns, err)
		}
	}()

	dir, err := ioutil.TempDir("", "cvo-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := createContent(filepath.Join(dir, "0.0.1"), version_0_0_1, map[string]string{"NAMESPACE": ns}); err != nil {
		t.Fatal(err)
	}
	if err := createContent(filepath.Join(dir, "0.0.2"), version_0_0_2_failing, map[string]string{"NAMESPACE": ns}); err != nil {
		t.Fatal(err)
	}
	payloadImage1 := "arbitrary/release:image"
	payloadImage2 := "arbitrary/release:image-2-failing"
	retriever := &mapPayloadRetriever{map[string]string{
		payloadImage1: filepath.Join(dir, "0.0.1"),
		payloadImage2: filepath.Join(dir, "0.0.2"),
	}}

	options := NewOptions()
	options.Namespace = ns
	options.Name = ns
	options.ListenAddr = ""
	options.NodeName = "test-node"
	options.ReleaseImage = payloadImage1
	options.PayloadOverride = filepath.Join(dir, "ignored")
	options.EnableMetrics = false
	controllers := options.NewControllerContext(cb)

	worker := cvo.NewSyncWorker(retriever, cvo.NewResourceBuilder(cfg), 5*time.Second, wait.Backoff{Steps: 3}).(*cvo.SyncWorker)
	controllers.CVO.SetSyncWorkerForTesting(worker)

	stopCh := make(chan struct{})
	defer close(stopCh)
	controllers.Start(stopCh)

	t.Logf("wait until we observe the cluster version become available")
	lastCV, err := waitForAvailableUpdate(t, client, ns, false, "0.0.1")
	if err != nil {
		t.Logf("latest version:\n%s", printCV(lastCV))
		t.Fatalf("cluster version never became available: %v", err)
	}

	t.Logf("verify the available cluster version's status matches our expectations")
	t.Logf("Cluster version:\n%s", printCV(lastCV))
	verifyClusterVersionStatus(t, lastCV, configv1.Update{Payload: payloadImage1, Version: "0.0.1"}, 1)
	verifyReleasePayload(t, kc, ns, "0.0.1", payloadImage1)

	t.Logf("trigger an update to a new version that should fail")
	cv, err := client.Config().ClusterVersions().Patch(ns, types.MergePatchType, []byte(fmt.Sprintf(`{"spec":{"desiredUpdate":{"payload":"%s"}}}`, payloadImage2)))
	if err != nil {
		t.Fatal(err)
	}
	if cv.Spec.DesiredUpdate == nil {
		t.Fatalf("cluster desired version was not preserved: %s", printCV(cv))
	}

	t.Logf("wait for operator to report failure")
	lastCV, err = waitUntilUpgradeFails(
		t, client, ns,
		"UpdatePayloadResourceInvalid",
		fmt.Sprintf(
			`Could not update configmap "%s/config2" (v1, 2 of 2): the object is invalid, possibly due to local cluster configuration`,
			ns,
		),
		"Unable to apply 0.0.2: some cluster configuration is invalid",
		"0.0.1", "0.0.2",
	)
	if err != nil {
		t.Logf("latest version:\n%s", printCV(lastCV))
		t.Fatalf("cluster version didn't report failure: %v", err)
	}

	t.Logf("ensure that one config map was updated and the other was not")
	verifyReleasePayloadConfigMap1(t, kc, ns, "0.0.2", payloadImage2)
	verifyReleasePayloadConfigMap2(t, kc, ns, "0.0.1", payloadImage1)

	t.Logf("switch back to 0.0.1 and verify it succeeds")
	cv, err = client.Config().ClusterVersions().Patch(ns, types.MergePatchType, []byte(`{"spec":{"desiredUpdate":{"payload":"", "version":"0.0.1"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if cv.Spec.DesiredUpdate == nil {
		t.Fatalf("cluster desired version was not preserved: %s", printCV(cv))
	}
	lastCV, err = waitForAvailableUpdate(t, client, ns, true, "0.0.2", "0.0.1")
	if err != nil {
		t.Logf("latest version:\n%s", printCV(lastCV))
		t.Fatalf("cluster version never reverted to 0.0.1: %v", err)
	}
	verifyClusterVersionStatus(t, lastCV, configv1.Update{Payload: payloadImage1, Version: "0.0.1"}, 3)
	verifyReleasePayload(t, kc, ns, "0.0.1", payloadImage1)
}

func TestIntegrationCVO_gracefulStepDown(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") != "1" {
		t.Skipf("Integration tests are disabled unless TEST_INTEGRATION=1")
	}
	t.Parallel()

	// use the same client setup as the start command
	cb, err := newClientBuilder("")
	if err != nil {
		t.Fatal(err)
	}
	cfg := cb.RestConfig()
	kc := cb.KubeClientOrDie("integration-test")
	client := cb.ClientOrDie("integration-test")

	ns := fmt.Sprintf("e2e-cvo-%s", randutil.String(4))

	if _, err := kc.Core().Namespaces().Create(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Config().ClusterVersions().Delete(ns, nil); err != nil {
			t.Logf("failed to delete cluster version %s: %v", ns, err)
		}
		if err := kc.Core().Namespaces().Delete(ns, nil); err != nil {
			t.Logf("failed to delete namespace %s: %v", ns, err)
		}
	}()

	options := NewOptions()
	options.Namespace = ns
	options.Name = ns
	options.ListenAddr = ""
	options.NodeName = "test-node"
	options.EnableMetrics = false
	controllers := options.NewControllerContext(cb)

	worker := cvo.NewSyncWorker(&mapPayloadRetriever{}, cvo.NewResourceBuilder(cfg), 5*time.Second, wait.Backoff{Steps: 3}).(*cvo.SyncWorker)
	controllers.CVO.SetSyncWorkerForTesting(worker)

	lock, err := createResourceLock(cb, ns, ns)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("the controller should create a lock record on a config map")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		options.run(ctx, controllers, lock)
		close(done)
	}()

	// wait until the lock record exists
	err = wait.PollImmediate(200*time.Millisecond, 60*time.Second, func() (bool, error) {
		_, err := kc.Core().ConfigMaps(ns).Get(ns, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("after the context is closed, the lock record should be deleted quickly")
	cancel()
	startTime := time.Now()
	var endTime time.Time
	// the lock should be deleted immediately
	err = wait.PollImmediate(100*time.Millisecond, 3*time.Second, func() (bool, error) {
		_, err := kc.Core().ConfigMaps(ns).Get(ns, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			endTime = time.Now()
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lock deleted in %s", endTime.Sub(startTime))

	select {
	case <-time.After(time.Second):
		t.Fatalf("controller should exit more quickly")
	case <-done:
	}
}

// waitForAvailableUpdates checks invariants during an upgrade process. versions is a list of the expected versions that
// should be seen during update, with the last version being the one we wait to see.
func waitForAvailableUpdate(t *testing.T, client clientset.Interface, ns string, allowIncrementalFailure bool, versions ...string) (*configv1.ClusterVersion, error) {
	var lastCV *configv1.ClusterVersion
	return lastCV, wait.PollImmediate(200*time.Millisecond, 60*time.Second, func() (bool, error) {
		cv, err := client.Config().ClusterVersions().Get(ns, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		lastCV = cv

		if !allowIncrementalFailure {
			if failing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorFailing); failing != nil && failing.Status == configv1.ConditionTrue {
				return false, fmt.Errorf("operator listed as failing (%s): %s", failing.Reason, failing.Message)
			}
		}

		// just wait until the operator is available
		if len(versions) == 0 {
			available := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorAvailable)
			return available != nil && available.Status == configv1.ConditionTrue, nil
		}

		if len(versions) == 1 {
			if available := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorAvailable); available == nil || available.Status == configv1.ConditionFalse {
				if progressing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorProgressing); available != nil && (progressing == nil || progressing.Status != configv1.ConditionTrue) {
					return false, fmt.Errorf("initializing operator should have progressing if available is false: %#v", progressing)
				}
				return false, nil
			}
			if len(cv.Status.History) == 0 {
				return false, fmt.Errorf("initializing operator should have history after available goes true")
			}
			if cv.Status.History[0].Version != versions[len(versions)-1] {
				return false, fmt.Errorf("initializing operator should report the target version in history once available")
			}
			if cv.Status.History[0].State != configv1.CompletedUpdate {
				return false, fmt.Errorf("initializing operator should report history completed %#v", cv.Status.History[0])
			}
			if progressing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorProgressing); progressing == nil || progressing.Status == configv1.ConditionTrue {
				return false, fmt.Errorf("initializing operator should never be available and still progressing or lacking the condition: %#v", progressing)
			}
			return true, nil
		}

		if available := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorAvailable); available == nil || available.Status == configv1.ConditionFalse {
			return false, fmt.Errorf("upgrading operator should remain available: %#v", available)
		}
		if !stringInSlice(versions, cv.Status.Desired.Version) {
			return false, fmt.Errorf("upgrading operator status reported desired version %s which is not in the allowed set %v", cv.Status.Desired.Version, versions)
		}
		if len(cv.Status.History) == 0 {
			return false, fmt.Errorf("upgrading operator should have at least once history entry")
		}
		if !stringInSlice(versions, cv.Status.History[0].Version) {
			return false, fmt.Errorf("upgrading operator should have a valid history[0] version %s: %v", cv.Status.Desired.Version, versions)
		}

		if cv.Status.History[0].Version != versions[len(versions)-1] {
			return false, nil
		}

		if failing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorFailing); failing != nil && failing.Status == configv1.ConditionTrue {
			return false, fmt.Errorf("operator listed as failing (%s): %s", failing.Reason, failing.Message)
		}

		progressing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorProgressing)
		if cv.Status.History[0].State != configv1.CompletedUpdate {
			if progressing == nil || progressing.Status != configv1.ConditionTrue {
				return false, fmt.Errorf("upgrading operator should have progressing true: %#v", progressing)
			}
			return false, nil
		}

		if progressing == nil || progressing.Status != configv1.ConditionFalse {
			return false, fmt.Errorf("upgraded operator should have progressing condition false: %#v", progressing)
		}
		return true, nil
	})
}

// waitUntilUpgradeFails checks invariants during an upgrade process. versions is a list of the expected versions that
// should be seen during update, with the last version being the one we wait to see.
func waitUntilUpgradeFails(t *testing.T, client clientset.Interface, ns string, failingReason, failingMessage, progressingMessage string, versions ...string) (*configv1.ClusterVersion, error) {
	var lastCV *configv1.ClusterVersion
	return lastCV, wait.PollImmediate(200*time.Millisecond, 60*time.Second, func() (bool, error) {
		cv, err := client.Config().ClusterVersions().Get(ns, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		lastCV = cv

		if c := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorAvailable); c == nil || c.Status != configv1.ConditionTrue {
			return false, fmt.Errorf("operator should remain available: %#v", c)
		}

		// just wait until the operator is failing
		if len(versions) == 0 {
			c := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorFailing)
			return c != nil && c.Status == configv1.ConditionTrue, nil
		}

		// TODO: add a test for initializing to an error state
		// if len(versions) == 1 {
		// 	if available := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorAvailable); available == nil || available.Status == configv1.ConditionFalse {
		// 		if progressing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorProgressing); available != nil && (progressing == nil || progressing.Status != configv1.ConditionTrue) {
		// 			return false, fmt.Errorf("initializing operator should have progressing if available is false: %#v", progressing)
		// 		}
		// 		return false, nil
		// 	}
		// 	if len(cv.Status.History) == 0 {
		// 		return false, fmt.Errorf("initializing operator should have history after available goes true")
		// 	}
		// 	if cv.Status.History[0].Version != versions[len(versions)-1] {
		// 		return false, fmt.Errorf("initializing operator should report the target version in history once available")
		// 	}
		// 	if cv.Status.History[0].State != configv1.CompletedUpdate {
		// 		return false, fmt.Errorf("initializing operator should report history completed %#v", cv.Status.History[0])
		// 	}
		// 	if progressing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorProgressing); progressing == nil || progressing.Status == configv1.ConditionTrue {
		// 		return false, fmt.Errorf("initializing operator should never be available and still progressing or lacking the condition: %#v", progressing)
		// 	}
		// 	return true, nil
		// }
		if len(versions) == 1 {
			return false, fmt.Errorf("unimplemented")
		}

		if !stringInSlice(versions, cv.Status.Desired.Version) {
			return false, fmt.Errorf("upgrading operator status reported desired version %s which is not in the allowed set %v", cv.Status.Desired.Version, versions)
		}
		if len(cv.Status.History) == 0 {
			return false, fmt.Errorf("upgrading operator should have at least once history entry")
		}
		if !stringInSlice(versions, cv.Status.History[0].Version) {
			return false, fmt.Errorf("upgrading operator should have a valid history[0] version %s: %v", cv.Status.Desired.Version, versions)
		}

		if cv.Status.History[0].Version != versions[len(versions)-1] {
			return false, nil
		}
		if cv.Status.History[0].State == configv1.CompletedUpdate {
			return false, fmt.Errorf("upgrading operator to failed payload should remain partial: %#v", cv.Status.History)
		}

		failing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorFailing)
		if failing == nil || failing.Status != configv1.ConditionTrue {
			return false, nil
		}
		progressing := resourcemerge.FindOperatorStatusCondition(cv.Status.Conditions, configv1.OperatorProgressing)
		if progressing == nil || progressing.Status != configv1.ConditionTrue {
			return false, fmt.Errorf("upgrading operator to failed payload should have progressing true: %#v", progressing)
		}
		if !strings.Contains(failing.Message, failingMessage) {
			return false, fmt.Errorf("failure message mismatch: %s", failing.Message)
		}
		if failing.Reason != failingReason {
			return false, fmt.Errorf("failure reason mismatch: %s", failing.Reason)
		}
		if progressing.Reason != failing.Reason {
			return false, fmt.Errorf("failure reason and progressing reason don't match: %s", progressing.Reason)
		}
		if !strings.Contains(progressing.Message, progressingMessage) {
			return false, fmt.Errorf("progressing message mismatch: %s", progressing.Message)
		}

		return true, nil
	})
}

func stringInSlice(slice []string, s string) bool {
	for _, item := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func verifyClusterVersionStatus(t *testing.T, cv *configv1.ClusterVersion, expectedUpdate configv1.Update, expectHistory int) {
	t.Helper()
	if cv.Status.Desired != expectedUpdate {
		t.Fatalf("unexpected: %#v", cv.Status.Desired)
	}
	if len(cv.Status.History) != expectHistory {
		t.Fatalf("unexpected: %#v", cv.Status.History)
	}
	actual := cv.Status.History[0]
	if actual.StartedTime.Time.IsZero() || actual.CompletionTime == nil || actual.CompletionTime.Time.IsZero() || actual.CompletionTime.Time.Before(actual.StartedTime.Time) {
		t.Fatalf("unexpected: %s -> %s", actual.StartedTime, actual.CompletionTime)
	}
	expect := configv1.UpdateHistory{
		State:          configv1.CompletedUpdate,
		Version:        expectedUpdate.Version,
		Payload:        expectedUpdate.Payload,
		StartedTime:    actual.StartedTime,
		CompletionTime: actual.CompletionTime,
	}
	if !reflect.DeepEqual(expect, actual) {
		t.Fatalf("unexpected history: %s", diff.ObjectReflectDiff(expect, actual))
	}
	if len(cv.Status.VersionHash) == 0 {
		t.Fatalf("unexpected version hash: %#v", cv.Status.VersionHash)
	}
	if cv.Status.Generation != cv.Generation {
		t.Fatalf("unexpected generation: %#v", cv.Status.Generation)
	}
}

func verifyReleasePayload(t *testing.T, kc kubernetes.Interface, ns, version, payload string) {
	t.Helper()
	verifyReleasePayloadConfigMap1(t, kc, ns, version, payload)
	verifyReleasePayloadConfigMap2(t, kc, ns, version, payload)
}

func verifyReleasePayloadConfigMap1(t *testing.T, kc kubernetes.Interface, ns, version, payload string) {
	t.Helper()
	cm, err := kc.CoreV1().ConfigMaps(ns).Get("config1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unable to find cm/config1 in ns %s: %v", ns, err)
	}
	if cm.Data["version"] != version || cm.Data["releaseImage"] != payload {
		t.Fatalf("unexpected cm/config1 contents: %#v", cm.Data)
	}
}

func verifyReleasePayloadConfigMap2(t *testing.T, kc kubernetes.Interface, ns, version, payload string) {
	t.Helper()
	cm, err := kc.CoreV1().ConfigMaps(ns).Get("config2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unable to find cm/config2 in ns %s: %v", ns, err)
	}
	if cm.Data["version"] != version || cm.Data["releaseImage"] != "{{.ReleaseImage}}" {
		t.Fatalf("unexpected cm/config2 contents: %#v", cm.Data)
	}
}

func printCV(cv *configv1.ClusterVersion) string {
	data, err := json.MarshalIndent(cv, "", "  ")
	if err != nil {
		return fmt.Sprintf("<error: %v>", err)
	}
	return string(data)
}

var reVariable = regexp.MustCompile(`\$\([a-zA-Z0-9_\-]+\)`)

func TestCreateContentReplacement(t *testing.T) {
	replacements := []map[string]string{
		{"NS": "other"},
	}
	in := `Some stuff $(NS) that should be $(NS)`
	out := reVariable.ReplaceAllStringFunc(in, func(key string) string {
		key = key[2 : len(key)-1]
		for _, r := range replacements {
			v, ok := r[key]
			if !ok {
				continue
			}
			return v
		}
		return key
	})
	if out != `Some stuff other that should be other` {
		t.Fatal(out)
	}
}

func createContent(baseDir string, content map[string]interface{}, replacements ...map[string]string) error {
	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return err
	}
	for k, v := range content {
		switch t := v.(type) {
		case string:
			if len(replacements) > 0 {
				t = reVariable.ReplaceAllStringFunc(t, func(key string) string {
					key = key[2 : len(key)-1]
					for _, r := range replacements {
						v, ok := r[key]
						if !ok {
							continue
						}
						return v
					}
					return key
				})
			}
			if err := ioutil.WriteFile(filepath.Join(baseDir, k), []byte(t), 0640); err != nil {
				return err
			}
		case map[string]interface{}:
			dir := filepath.Join(baseDir, k)
			if err := os.Mkdir(dir, 0750); err != nil {
				return err
			}
			if err := createContent(dir, t, replacements...); err != nil {
				return err
			}
		}
	}
	return nil
}

type mapPayloadRetriever struct {
	Paths map[string]string
}

func (r *mapPayloadRetriever) RetrievePayload(ctx context.Context, update configv1.Update) (string, error) {
	path, ok := r.Paths[update.Payload]
	if !ok {
		return "", fmt.Errorf("no payload found for %q", update.Payload)
	}
	return path, nil
}
