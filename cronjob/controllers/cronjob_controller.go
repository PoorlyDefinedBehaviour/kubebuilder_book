/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "tutorial.kubebuilder.io/cronjob/api/v1"
)

const (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
	jobOwnerKey             = ".metadata.controller"
)

var (
	apiGVstr = batchv1.GroupVersion.String()
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// CLock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.1/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cronJob := batchv1.CronJob{}
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		logger.Error(err, "unable to fetch CronJob")
		// We'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	childJobs := kbatch.JobList{}
	if err := r.List(
		ctx,
		&childJobs,
		client.InNamespace(req.Namespace),
		client.MatchingFields{jobOwnerKey: req.Name},
	); err != nil {
		logger.Error(err, "unable to list child jobs")
		return ctrl.Result{}, err
	}

	// Find the active list of jobs
	var activeJobs []*kbatch.Job
	var successsfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	// Find the last run so we can update the status
	var mostRecentTime *time.Time

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)

		switch finishedType {
		// Not finished
		case "":
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successsfulJobs = append(successsfulJobs, &childJobs.Items[i])
		}

		// We'll store the launch time in an annotation, so we'll reconstitute
		// that from the active jobs themselves.
		scheduledTimeForJob, err := getScheduleTimeForJob(&job)
		if err != nil {
			logger.Error(err, "unable to parsed schedule tiem for child job", "job", &job)
			continue
		}
		// Save the most recent scheduled time.
		if !scheduledTimeForJob.IsZero() {
			if mostRecentTime == nil {
				mostRecentTime = &scheduledTimeForJob
			} else if mostRecentTime.Before(scheduledTimeForJob) {
				mostRecentTime = &scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}

	cronJob.Status.Active = nil

	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			logger.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	log.Log.V(1).Info(
		"job count",
		"active jobs", len(activeJobs),
		"successful jobs", len(successsfulJobs),
		"failed jobs", len(failedJobs),
	)

	if err := r.Status().Update(ctx, &cronJob); err != nil {
		logger.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	// NB: deleting these are "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		// Sort jobs by start time in ascending order.
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})

		for i, job := range failedJobs {
			// Remove just the right amount of failed jobs to stay under the failed jobs history limit.
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}

			if err :=
				r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				logger.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.Log.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		// Sort jobs by start time in ascending order.
		sort.Slice(successsfulJobs, func(i, j int) bool {
			if successsfulJobs[i].Status.StartTime == nil {
				return successsfulJobs[j].Status.StartTime == nil
			}
			return successsfulJobs[i].Status.StartTime.Before(successsfulJobs[j].Status.StartTime)
		})

		for i, job := range successsfulJobs {
			// Remove just the right amount of successful jobs to stay under the successful jobs history limit.
			if int32(i) >= int32(len(successsfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}

			if err :=
				r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				logger.Error(err, "unable to delete old successful job", "job", job)
			} else {
				log.Log.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.Log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// Figure out the next times that we need to create jobs at (or anything we missed)
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Clock.Now())
	if err != nil {
		logger.Error(err, "unable to figure out CronJob schedule")
		// We don't really care about requeing until we get an update that
		// fixes the schedule, so don't return an error.
		return ctrl.Result{}, nil
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Clock.Now())}
	logger = logger.WithValues("now", r.Clock.Now(), "next run", nextRun)

	if missedRun.IsZero() {
		log.Log.V(1).Info("no upcoming schedule times, sleeping until next")
		return scheduledResult, nil
	}

	logger = logger.WithValues("current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Clock.Now())
	}

	if tooLate {
		log.Log.V(1).Info("missed starting deadline for last run, sleeping till next")
		return scheduledResult, nil
	}

	// Figre out how to run this job -- concurrency policy might for bid us from running
	// multiple jobs at the same time.
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.Log.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduledResult, nil
	}

	// The concurrency policy can also instruct us to replace existing ones.
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			// We don't care if the job was already deleted.
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				logger.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
	}

	job, err := r.constructJobForCronJob(&cronJob, missedRun)
	if err != nil {
		logger.Error(err, "unable to construct job from template")
		// Don't bother requeuing until we get a change to the spec.
		return scheduledResult, nil
	}

	if err := r.Create(ctx, job); err != nil {
		logger.Error(err, "unable to create Job for CronJob", "job", job)
		return ctrl.Result{}, err
	}

	log.Log.V(1).Info("created Job", "job", job)

	return scheduledResult, nil
}

func isJobFinished(job *kbatch.Job) (bool, kbatch.JobConditionType) {
	for _, condition := range job.Status.Conditions {
		if (condition.Type == kbatch.JobComplete || condition.Type == kbatch.JobFailed) && condition.Status == corev1.ConditionTrue {
			return true, condition.Type
		}
	}

	return false, ""
}

func getScheduleTimeForJob(job *kbatch.Job) (timeParsed time.Time, err error) {
	timeRaw := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 {
		return timeParsed, nil
	}

	timeParsed, err = time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return timeParsed, err
	}

	return timeParsed, nil
}

func getNextSchedule(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
	sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
	if err != nil {
		return lastMissed, next, fmt.Errorf("unparseable schedule %q: %v", cronJob.Spec.Schedule, err)
	}

	// For optimization purposes, cheat a bit and start from our last observed run time
	// we could reconstitute this here, but there's not much point, since we've
	// just updated it.
	var earliestTime time.Time
	if cronJob.Status.LastScheduleTime != nil {
		earliestTime = cronJob.Status.LastScheduleTime.Time
	} else {
		earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
	}

	if cronJob.Spec.StartingDeadlineSeconds != nil {
		// Controller is not going to schedule anything below this point.
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}

	if earliestTime.After(now) {

		return lastMissed, sched.Next(now), nil
	}

	starts := 0

	for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
		lastMissed = t
		// An object might miss several starts. For example, if
		// controller gets wedged on Friday at 5:01pm when everyone has
		// gone home, and someone comes in on Tuesday AM and discovers
		// the problem and restarts the controller, then all the hourly
		// jobs, more than 80 of them for one hourly scheduledJob, should
		// all start running with no further intervention (if the scheduledJob
		// allows concurrency and late starts).
		//
		// However, if there is a bug somewhere, or incorrect clock
		// on controller's server or apiservers (for setting creationTimestamp)
		// then there could be so many missed start times (it could be off
		// by decades or more), that it would eat up all the CPU and memory
		// of this controller. In that case, we want to not try to list
		// all the missed start times.
		starts += 1
		if starts > 100 {
			// We can't get the most recent time so just return an empty slice.
			return lastMissed, next, fmt.Errorf("too many missed start times(> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew.")
		}
	}

	return lastMissed, sched.Next(now), nil
}

func (r *CronJobReconciler) constructJobForCronJob(cronJob *batchv1.CronJob, scheduleTime time.Time) (*kbatch.Job, error) {
	// We want job names for a given nominal start time to have a
	// deterministic name to avoid the same job being created twice.
	name := fmt.Sprintf("%s-%d", cronJob.Name, scheduleTime.Unix())

	job := &kbatch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
			Name:        name,
			Namespace:   cronJob.Namespace,
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}

	for k, v := range cronJob.Spec.JobTemplate.Annotations {
		job.Labels[k] = v
	}

	if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
		return nil, err
	}

	return job, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Set up a real clock, since we're not in a test.
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObject client.Object) []string {
		// Grab the job object, extract the owner.
		job := rawObject.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}

		// Make sure it is owned by a CronJob.
		if owner.APIVersion != apiGVstr || owner.Kind != "CronJob" {
			return nil
		}

		return []string{owner.Name}
	})
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Complete(r)
}
