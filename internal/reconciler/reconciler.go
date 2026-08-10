package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	notificationsv1 "github.com/agynio/expose/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/hostname"
	"github.com/agynio/expose/internal/naming"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	notificationRetryDelay = 5 * time.Second
	workloadRoomPrefix     = "workload:"
)

var notificationRoomPollInterval = notificationRetryDelay

// NameResolver reads the labels an exposure address is built from.
type NameResolver interface {
	Resolve(ctx context.Context, target naming.Target) (string, hostname.Owner, error)
}

type ReconcilerStore interface {
	ListExposuresByStatus(ctx context.Context, status store.ExposureStatus) ([]store.Exposure, error)
	UpdateExposureAddress(ctx context.Context, id uuid.UUID, host string, url string) error
	UpdateExposureOwner(ctx context.Context, id uuid.UUID, owner store.ExposureOwner) error
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
	names         NameResolver
	interval      time.Duration
}

func New(
	store ReconcilerStore,
	zitiMgmt zitimanagementv1.ZitiManagementServiceClient,
	runners runnersv1.RunnersServiceClient,
	notifications notificationsv1.NotificationsServiceClient,
	names NameResolver,
	interval time.Duration,
) *Reconciler {
	return &Reconciler{
		store:         store,
		zitiMgmt:      zitiMgmt,
		runners:       runners,
		notifications: notifications,
		names:         names,
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
	rooms, err := r.activeWorkloadRooms(ctx)
	if err != nil {
		return err
	}
	for len(rooms) > 0 {
		subscribeCtx, cancel := context.WithCancel(ctx)
		roomsUpdated := make(chan []string, 1)
		roomsErrs := make(chan error, 1)
		go r.watchActiveWorkloadRooms(subscribeCtx, rooms, roomsUpdated, roomsErrs, cancel)

		stream, err := r.notifications.Subscribe(subscribeCtx, &notificationsv1.SubscribeRequest{Rooms: rooms})
		if err != nil {
			cancel()
			return err
		}
		resubscribe := false
		for {
			resp, err := stream.Recv()
			if err != nil {
				cancel()
				if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
					if ctx.Err() != nil {
						return nil
					}
					select {
					case err := <-roomsErrs:
						return err
					default:
					}
					select {
					case rooms = <-roomsUpdated:
						resubscribe = true
					default:
					}
					break
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
		if !resubscribe {
			return nil
		}
		if len(rooms) == 0 {
			return nil
		}
	}
	return nil
}

func (r *Reconciler) watchActiveWorkloadRooms(
	ctx context.Context,
	rooms []string,
	roomsUpdated chan<- []string,
	roomsErrs chan<- error,
	cancel context.CancelFunc,
) {
	ticker := time.NewTicker(notificationRoomPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nextRooms, err := r.activeWorkloadRooms(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				select {
				case roomsErrs <- err:
				default:
				}
				cancel()
				return
			}
			if roomsEqual(rooms, nextRooms) {
				continue
			}
			select {
			case roomsUpdated <- nextRooms:
			default:
			}
			cancel()
			return
		}
	}
}

func (r *Reconciler) activeWorkloadRooms(ctx context.Context) ([]string, error) {
	workloadIDs, err := r.store.ListAllActiveWorkloadIDs(ctx)
	if err != nil {
		return nil, err
	}
	rooms := make([]string, 0, len(workloadIDs))
	for _, workloadID := range workloadIDs {
		rooms = append(rooms, workloadRoom(workloadID))
	}
	sort.Strings(rooms)
	return rooms, nil
}

func (r *Reconciler) reconcile(ctx context.Context) {
	r.reconcileActive(ctx)
	r.reconcileFailed(ctx)
	r.reconcileRemoving(ctx)
}

// reconcileActive walks every live exposure once, for two jobs that need the
// same workload record: retiring exposures whose workload is gone, and carrying
// a rename through to those whose workload is still there.
func (r *Reconciler) reconcileActive(ctx context.Context) {
	exposures, err := r.store.ListExposuresByStatus(ctx, store.ExposureStatusActive)
	if err != nil {
		log.Printf("reconcile active: list active exposures: %v", err)
		return
	}
	for _, exposure := range exposures {
		workloadCtx := contextWithExposureIdentity(ctx, exposure)
		resp, err := r.runners.GetWorkload(workloadCtx, &runnersv1.GetWorkloadRequest{Id: exposure.WorkloadID.String()})
		if err == nil {
			if !isTerminalWorkload(resp.GetWorkload()) {
				r.reconcileAddress(ctx, exposure, resp.GetWorkload())
				continue
			}
		} else if !isNotFound(err) {
			log.Printf("reconcile active: get workload %s: %v", exposure.WorkloadID, err)
			continue
		}
		if err := r.store.UpdateExposureStatus(ctx, exposure.ID, store.ExposureStatusRemoving); err != nil {
			log.Printf("reconcile active: update exposure %s: %v", exposure.ID, err)
			continue
		}
		r.removeExposure(ctx, exposure)
	}
}

// reconcileAddress re-derives an exposure's address and rewrites its
// intercept.v1 config when it has drifted. This is what carries an
// organization, sandbox, or instance rename through to a live exposure.
//
// Renames are picked up here rather than by an event: this pass already re-reads
// every live exposure, and an address stale for one interval is a cosmetic delay
// rather than a fault. The address a caller already holds keeps working until
// the rewrite lands, then stops.
func (r *Reconciler) reconcileAddress(ctx context.Context, exposure store.Exposure, workload *runnersv1.Workload) {
	if r.names == nil {
		return
	}
	// Rows migrated from the agent-shaped schema could not know their
	// organization, so the workload record is the source for both.
	if owner, ok := ownerFromWorkload(workload); ok && !ownerMatches(exposure, owner) {
		if err := r.store.UpdateExposureOwner(ctx, exposure.ID, owner); err != nil {
			log.Printf("reconcile address: update owner for exposure %s: %v", exposure.ID, err)
			return
		}
		exposure.OwnerKind = owner.OwnerKind
		exposure.OwnerID = owner.OwnerID
		exposure.AgentID = owner.AgentID
		exposure.OrganizationID = owner.OrganizationID
	}

	desired, err := r.deriveHostname(ctx, exposure)
	if err != nil {
		log.Printf("reconcile address: derive hostname for exposure %s: %v", exposure.ID, err)
		return
	}
	if desired == exposure.Hostname {
		return
	}

	if _, err := r.zitiMgmt.UpdateService(ctx, &zitimanagementv1.UpdateServiceRequest{
		ZitiServiceId: exposure.OpenZitiServiceID,
		InterceptV1Config: &zitimanagementv1.InterceptV1Config{
			Protocols:  []string{"tcp"},
			Addresses:  []string{desired},
			PortRanges: []*zitimanagementv1.PortRange{{Low: exposure.Port, High: exposure.Port}},
		},
	}); err != nil {
		log.Printf("reconcile address: rewrite intercept for exposure %s: %v", exposure.ID, err)
		return
	}
	if err := r.store.UpdateExposureAddress(ctx, exposure.ID, desired, fmt.Sprintf("http://%s:%d", desired, exposure.Port)); err != nil {
		log.Printf("reconcile address: store address for exposure %s: %v", exposure.ID, err)
		return
	}
	log.Printf("exposure %s moved from %s to %s", exposure.ID, exposure.Hostname, desired)
}

func (r *Reconciler) deriveHostname(ctx context.Context, exposure store.Exposure) (string, error) {
	if !exposure.OrganizationID.Valid {
		return hostname.Fallback(exposure.ID), nil
	}
	slug, owner, err := r.names.Resolve(ctx, naming.Target{
		OrganizationID: exposure.OrganizationID.UUID,
		OwnerKind:      toHostnameOwnerKind(exposure.OwnerKind),
		OwnerID:        exposure.OwnerID,
	})
	if err != nil {
		return "", err
	}
	return hostname.Derive(exposure.ID, slug, owner), nil
}

func ownerFromWorkload(workload *runnersv1.Workload) (store.ExposureOwner, bool) {
	if workload == nil {
		return store.ExposureOwner{}, false
	}
	ownerID, err := uuid.Parse(strings.TrimSpace(workload.GetOwnerId()))
	if err != nil {
		return store.ExposureOwner{}, false
	}
	orgID, err := uuid.Parse(strings.TrimSpace(workload.GetOrganizationId()))
	if err != nil {
		return store.ExposureOwner{}, false
	}
	var kind store.OwnerKind
	switch workload.GetOwnerKind() {
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE:
		kind = store.OwnerKindAgentInstance
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX:
		kind = store.OwnerKindSandbox
	default:
		return store.ExposureOwner{}, false
	}
	owner := store.ExposureOwner{
		OwnerKind:      kind,
		OwnerID:        ownerID,
		OrganizationID: uuid.NullUUID{UUID: orgID, Valid: true},
	}
	if classID, err := uuid.Parse(strings.TrimSpace(workload.GetAgentClassId())); err == nil {
		owner.AgentID = uuid.NullUUID{UUID: classID, Valid: true}
	}
	return owner, true
}

func ownerMatches(exposure store.Exposure, owner store.ExposureOwner) bool {
	return exposure.OwnerKind == owner.OwnerKind &&
		exposure.OwnerID == owner.OwnerID &&
		exposure.AgentID == owner.AgentID &&
		exposure.OrganizationID == owner.OrganizationID
}

func toHostnameOwnerKind(kind store.OwnerKind) hostname.OwnerKind {
	switch kind {
	case store.OwnerKindAgentInstance:
		return hostname.OwnerKindAgentInstance
	case store.OwnerKindSandbox:
		return hostname.OwnerKindSandbox
	default:
		return hostname.OwnerKindUnspecified
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
	exposures, err := r.store.ListExposuresByWorkloadAll(ctx, workloadID)
	if err != nil {
		log.Printf("reconcile workload %s: list exposures: %v", workloadID, err)
		return
	}
	if len(exposures) == 0 {
		return
	}
	workloadCtx := contextWithExposureIdentity(ctx, exposures[0])
	resp, err := r.runners.GetWorkload(workloadCtx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err == nil {
		if !isTerminalWorkload(resp.GetWorkload()) {
			return
		}
	} else if !isNotFound(err) {
		log.Printf("reconcile workload %s: get workload: %v", workloadID, err)
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
		return true
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
	if !strings.HasPrefix(room, workloadRoomPrefix) {
		return uuid.UUID{}, false
	}
	value := strings.TrimPrefix(room, workloadRoomPrefix)
	if value == "" {
		return uuid.UUID{}, false
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.UUID{}, false
	}
	return id, true
}

func workloadRoom(workloadID uuid.UUID) string {
	return workloadRoomPrefix + workloadID.String()
}

func roomsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for idx, room := range left {
		if room != right[idx] {
			return false
		}
	}
	return true
}
