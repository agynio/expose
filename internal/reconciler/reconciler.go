package reconciler

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	notificationsv1 "github.com/agynio/expose/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const notificationRetryDelay = 5 * time.Second

type ReconcilerStore interface {
	ListExposuresByStatus(ctx context.Context, status store.ExposureStatus) ([]store.Exposure, error)
	ListExposuresByWorkloadAll(ctx context.Context, workloadID uuid.UUID) ([]store.Exposure, error)
	ListAllActiveWorkloadIDs(ctx context.Context) ([]uuid.UUID, error)
	UpdateExposureStatus(ctx context.Context, id uuid.UUID, status store.ExposureStatus) error
	DeleteExposure(ctx context.Context, id uuid.UUID) error
}

type Reconciler struct {
	store         ReconcilerStore
	zitiMgmt      zitimanagementv1.ZitiManagementServiceClient
	runners       runnersv1.RunnersServiceClient
	notifications notificationsv1.NotificationsServiceClient
	interval      time.Duration
}

func New(
	store ReconcilerStore,
	zitiMgmt zitimanagementv1.ZitiManagementServiceClient,
	runners runnersv1.RunnersServiceClient,
	notifications notificationsv1.NotificationsServiceClient,
	interval time.Duration,
) *Reconciler {
	return &Reconciler{
		store:         store,
		zitiMgmt:      zitiMgmt,
		runners:       runners,
		notifications: notifications,
		interval:      interval,
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	if r.notifications != nil {
		go r.listenNotifications(ctx)
	}
	r.reconcile(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context) {
	r.reconcile(ctx)
}

func (r *Reconciler) listenNotifications(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.subscribeAndProcess(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("notifications subscribe error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(notificationRetryDelay):
		}
	}
}

func (r *Reconciler) subscribeAndProcess(ctx context.Context) error {
	stream, err := r.notifications.Subscribe(ctx, &notificationsv1.SubscribeRequest{})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}
		envelope := resp.GetEnvelope()
		if envelope == nil {
			continue
		}
		if envelope.GetEvent() != "workload.status_changed" {
			continue
		}
		for _, room := range envelope.GetRooms() {
			workloadID, ok := parseWorkloadRoom(room)
			if !ok {
				continue
			}
			r.reconcileWorkload(ctx, workloadID)
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) {
	r.reconcileOrphaned(ctx)
	r.reconcileFailed(ctx)
	r.reconcileRemoving(ctx)
}

func (r *Reconciler) reconcileOrphaned(ctx context.Context) {
	exposures, err := r.store.ListExposuresByStatus(ctx, store.ExposureStatusActive)
	if err != nil {
		log.Printf("reconcile orphaned: list active exposures: %v", err)
		return
	}
	for _, exposure := range exposures {
		resp, err := r.runners.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: exposure.WorkloadID.String()})
		if err == nil {
			if !isTerminalWorkload(resp.GetWorkload()) {
				continue
			}
		} else if !isNotFound(err) {
			log.Printf("reconcile orphaned: get workload %s: %v", exposure.WorkloadID, err)
			continue
		}
		if err := r.store.UpdateExposureStatus(ctx, exposure.ID, store.ExposureStatusRemoving); err != nil {
			log.Printf("reconcile orphaned: update exposure %s: %v", exposure.ID, err)
			continue
		}
		r.removeExposure(ctx, exposure)
	}
}

func (r *Reconciler) reconcileFailed(ctx context.Context) {
	exposures, err := r.store.ListExposuresByStatus(ctx, store.ExposureStatusFailed)
	if err != nil {
		log.Printf("reconcile failed: list failed exposures: %v", err)
		return
	}
	for _, exposure := range exposures {
		r.removeExposure(ctx, exposure)
	}
}

func (r *Reconciler) reconcileRemoving(ctx context.Context) {
	exposures, err := r.store.ListExposuresByStatus(ctx, store.ExposureStatusRemoving)
	if err != nil {
		log.Printf("reconcile removing: list removing exposures: %v", err)
		return
	}
	for _, exposure := range exposures {
		r.removeExposure(ctx, exposure)
	}
}

func (r *Reconciler) reconcileWorkload(ctx context.Context, workloadID uuid.UUID) {
	resp, err := r.runners.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err == nil {
		if !isTerminalWorkload(resp.GetWorkload()) {
			return
		}
	} else if !isNotFound(err) {
		log.Printf("reconcile workload %s: get workload: %v", workloadID, err)
		return
	}
	exposures, err := r.store.ListExposuresByWorkloadAll(ctx, workloadID)
	if err != nil {
		log.Printf("reconcile workload %s: list exposures: %v", workloadID, err)
		return
	}
	for _, exposure := range exposures {
		r.removeExposure(ctx, exposure)
	}
}

func (r *Reconciler) removeExposure(ctx context.Context, exposure store.Exposure) {
	if err := r.deleteServicePolicy(ctx, exposure.OpenZitiDialPolicyID); err != nil {
		log.Printf("remove exposure %s: delete dial policy: %v", exposure.ID, err)
		return
	}
	if err := r.deleteServicePolicy(ctx, exposure.OpenZitiBindPolicyID); err != nil {
		log.Printf("remove exposure %s: delete bind policy: %v", exposure.ID, err)
		return
	}
	if err := r.deleteService(ctx, exposure.OpenZitiServiceID); err != nil {
		log.Printf("remove exposure %s: delete service: %v", exposure.ID, err)
		return
	}
	if err := r.store.DeleteExposure(ctx, exposure.ID); err != nil {
		log.Printf("remove exposure %s: delete exposure: %v", exposure.ID, err)
	}
}

func (r *Reconciler) deleteServicePolicy(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := r.zitiMgmt.DeleteServicePolicy(ctx, &zitimanagementv1.DeleteServicePolicyRequest{ZitiServicePolicyId: id})
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

func (r *Reconciler) deleteService(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := r.zitiMgmt.DeleteService(ctx, &zitimanagementv1.DeleteServiceRequest{ZitiServiceId: id})
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

func isTerminalWorkload(workload *runnersv1.Workload) bool {
	if workload == nil {
		return false
	}
	if workload.GetRemovedAt() != nil {
		return true
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return true
	default:
		return false
	}
}

func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

func parseWorkloadRoom(room string) (uuid.UUID, bool) {
	const prefix = "workload:"
	if !strings.HasPrefix(room, prefix) {
		return uuid.UUID{}, false
	}
	value := strings.TrimPrefix(room, prefix)
	if value == "" {
		return uuid.UUID{}, false
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.UUID{}, false
	}
	return id, true
}
