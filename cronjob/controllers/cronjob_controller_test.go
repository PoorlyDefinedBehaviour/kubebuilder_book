package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cronjobv1 "tutorial.kubebuilder.io/cronjob/api/v1"
)

var _ = Describe("CronJob controller", func() {
	const (
		cronJobName      = "test-cronjob"
		cronJobNamespace = "default"
		jobName          = "test-job"

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

	Context("When updating CronJob Status", func() {
		It("Should increase CronJob Status.Active count when new Jobs are created", func() {
			By("Creating a new CronJob")

			ctx := context.Background()

			cronJob := cronjobv1.CronJob{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "batch.tutorial.kubebuilder.io/v1",
					Kind:       "CronJob",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      cronJobName,
					Namespace: cronJobNamespace,
				},
				Spec: cronjobv1.CronJobSpec{
					Schedule: "1 * * * *",
					JobTemplate: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "test-container",
											Image: "test-image",
										},
									},
									RestartPolicy: v1.RestartPolicyOnFailure,
								},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, &cronJob)).Should(Succeed())

			cronjobLookupKey := types.NamespacedName{Name: cronJobName, Namespace: cronJob.Namespace}
			createdCronjob := cronjobv1.CronJob{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, cronjobLookupKey, &createdCronjob)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(createdCronjob.Spec.Schedule).Should(Equal("1 * * * *"))

			By("By checking the CronJob has zero active jobs")
			Consistently(func() (int, error) {
				if err := k8sClient.Get(ctx, cronjobLookupKey, &createdCronjob); err != nil {
					return -1, err
				}
				return len(createdCronjob.Status.Active), nil
			}, duration, interval).Should(Equal(0))

			By("By creating a new Job")
			testJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: cronJobNamespace,
				},
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
								},
							},
							RestartPolicy: v1.RestartPolicyOnFailure,
						},
					},
				},
				Status: batchv1.JobStatus{
					Active: 2,
				},
			}

			kind := reflect.TypeOf(cronjobv1.CronJob{}).Name()
			gvk := cronjobv1.GroupVersion.WithKind(kind)

			controllerRef := metav1.NewControllerRef(&createdCronjob, gvk)
			testJob.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
			Expect(k8sClient.Create(ctx, &testJob)).Should(Succeed())

			By("By checking that the CronJob has one active Job")
			Eventually(func() ([]string, error) {
				if err := k8sClient.Get(ctx, cronjobLookupKey, &createdCronjob); err != nil {
					return nil, err
				}

				names := []string{}

				for _, job := range createdCronjob.Status.Active {
					names = append(names, job.Name)
				}

				return names, nil
			},
				timeout, interval,
			).
				Should(ConsistOf(jobName))
		})
	})
})
