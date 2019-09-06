package cvo

import (
	"fmt"
	"sort"
	"strings"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	configlistersv1 "github.com/openshift/client-go/config/listers/config/v1"
	"github.com/openshift/cluster-version-operator/lib/resourcemerge"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"
)

// syncUpgradeable. The status is only checked if it has been more than
// the minimumUpdateCheckInterval since the last check.
func (optr *Operator) syncUpgradeable(config *configv1.ClusterVersion) error {
	// updates are only checked at most once per minimumUpdateCheckInterval or if the generation changes
	u := optr.getUpgradeable()
	if u != nil && u.RecentlyChanged(optr.minimumUpdateCheckInterval) {
		klog.V(4).Infof("Upgradeable conditions were recently checked, will try later.")
		return nil
	}

	now := metav1.Now()
	var conds []configv1.ClusterOperatorStatusCondition
	var reasons []string
	for _, check := range optr.upgradeableChecks {
		if cond := check.Check(); cond != nil {
			reasons = append(reasons, cond.Reason)
			cond.LastTransitionTime = now
			conds = append(conds, *cond)
		}
	}
	if len(conds) == 0 {
		return nil
	} else if len(conds) == 1 {
		conds = append(conds, configv1.ClusterOperatorStatusCondition{
			Type:               configv1.OperatorUpgradeable,
			Status:             configv1.ConditionFalse,
			Reason:             conds[0].Reason,
			Message:            conds[0].Message,
			LastTransitionTime: now,
		})
	} else {
		conds = append(conds, configv1.ClusterOperatorStatusCondition{
			Type:               configv1.OperatorUpgradeable,
			Status:             configv1.ConditionFalse,
			Reason:             "MultipleReasons",
			Message:            fmt.Sprintf("Cluster is not Upgradeable due to multiple reasons: %s", strings.Join(reasons, ",")),
			LastTransitionTime: now,
		})
	}
	sort.Slice(conds, func(i, j int) bool { return conds[i].Type < conds[j].Type })
	optr.setUpgradeable(&upgradeable{
		Conditions: conds,
	})
	// requeue
	optr.queue.Add(optr.queueKey())
	return nil
}

type upgradeable struct {
	At time.Time

	// these are sorted by Type
	Conditions []configv1.ClusterOperatorStatusCondition
}

func (u *upgradeable) RecentlyChanged(interval time.Duration) bool {
	return u.At.After(time.Now().Add(-interval))
}

func (u *upgradeable) NeedsUpdate(original *configv1.ClusterVersion) *configv1.ClusterVersion {
	if u == nil {
		return nil
	}

	origUpConditions := collectUpgradeableConditions(original.Status.Conditions)
	if equality.Semantic.DeepEqual(u.Conditions, origUpConditions) {
		return nil
	}

	config := original.DeepCopy()
	for _, c := range u.Conditions {
		resourcemerge.SetOperatorStatusCondition(&config.Status.Conditions, c)
	}
	for _, origc := range origUpConditions {
		if c := resourcemerge.FindOperatorStatusCondition(u.Conditions, origc.Type); c == nil {
			resourcemerge.RemoveOperatorStatusCondition(&config.Status.Conditions, origc.Type)
		}
	}
	return config
}

func collectUpgradeableConditions(conditions []configv1.ClusterOperatorStatusCondition) []configv1.ClusterOperatorStatusCondition {
	var ret []configv1.ClusterOperatorStatusCondition
	for _, c := range conditions {
		if strings.HasPrefix(string(c.Type), string(configv1.OperatorUpgradeable)) {
			ret = append(ret, c)
		}
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Type < ret[j].Type })
	return ret
}

// setUpgradeable updates the currently calculated status of Upgradeable
func (optr *Operator) setUpgradeable(u *upgradeable) {
	if u != nil {
		u.At = time.Now()
	}

	optr.upgradeableStatusLock.Lock()
	defer optr.upgradeableStatusLock.Unlock()
	optr.upgradeable = u
}

// getUpgradeable returns the current calculated status of upgradeable. It
// may be nil.
func (optr *Operator) getUpgradeable() *upgradeable {
	optr.upgradeableStatusLock.Lock()
	defer optr.upgradeableStatusLock.Unlock()
	return optr.upgradeable
}

type upgradeableCheck interface {
	// returns a not-nil condition when the check fails.
	Check() *configv1.ClusterOperatorStatusCondition
}

type clusterOperatorsUpgradeable struct {
	coLister configlistersv1.ClusterOperatorLister
}

func (check *clusterOperatorsUpgradeable) Check() *configv1.ClusterOperatorStatusCondition {
	cond := &configv1.ClusterOperatorStatusCondition{
		Type:   configv1.ClusterStatusConditionType("UpgradeableClusterOperators"),
		Status: configv1.ConditionFalse,
	}
	ops, err := check.coLister.List(labels.Everything())
	if meta.IsNoMatchError(err) {
		return nil
	}
	if err != nil {
		cond.Reason = "FailedToListClusterOperators"
		cond.Message = errors.Wrap(err, "failed to list cluster operators").Error()
		return cond
	}

	var notup []string
	for _, op := range ops {
		if up := resourcemerge.FindOperatorStatusCondition(op.Status.Conditions, configv1.OperatorUpgradeable); up != nil && up.Status == configv1.ConditionFalse {
			notup = append(notup, fmt.Sprintf("Cluster Operator %q is not Upgradeable(%q): %s", op.GetName(), up.Reason, up.Message))
		}
	}

	if len(notup) == 0 {
		return nil
	}
	msg := ""
	reason := ""
	if len(notup) == 1 {
		msg = notup[0]
		reason = "ClusterOperatorNotUpgradeable"
	} else {
		msg = fmt.Sprintf("Multiple Cluster Operators are not upgradeable:\n* %s", strings.Join(notup, "\n* "))
		reason = "ClusterOperatorsNotUpgradeable"
	}
	cond.Reason = reason
	cond.Message = msg
	return cond
}

type clusterVersionOverridesUpgradebale struct {
	name     string
	cvLister configlistersv1.ClusterVersionLister
}

func (check *clusterVersionOverridesUpgradebale) Check() *configv1.ClusterOperatorStatusCondition {
	cond := &configv1.ClusterOperatorStatusCondition{
		Type:   configv1.ClusterStatusConditionType("UpgradeableClusterVersionOverrides"),
		Status: configv1.ConditionFalse,
	}

	cv, err := check.cvLister.Get(check.name)
	if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
		return nil
	}

	overrides := false
	for _, o := range cv.Spec.Overrides {
		if o.Unmanaged {
			overrides = true
		}
	}
	if !overrides {
		return nil
	}

	cond.Reason = "ClusterVersionOverridesSet"
	cond.Message = "Cluster Version overrides is being used to unmanage certain owned resources."
	return cond
}

func (optr *Operator) defaultUpgradeableChecks() []upgradeableCheck {
	return []upgradeableCheck{
		&clusterOperatorsUpgradeable{coLister: optr.coLister},
		&clusterVersionOverridesUpgradebale{name: optr.name, cvLister: optr.cvLister},
	}
}
