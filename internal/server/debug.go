package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	debugExposurePathPrefix = "/debug/ziti/exposures/"
	debugTokenHeader        = "X-Expose-Debug-Token"
)

type DebugHTTPServer struct {
	store    ExposureStore
	zitiMgmt zitimanagementv1.ZitiManagementServiceClient
	token    string
}

type DebugExposureState struct {
	ExposureID       string               `json:"exposure_id"`
	ServiceName      string               `json:"service_name"`
	ZitiServiceID    string               `json:"ziti_service_id"`
	ZitiBindPolicyID string               `json:"ziti_bind_policy_id"`
	ZitiDialPolicyID string               `json:"ziti_dial_policy_id"`
	ZitiService      DebugZitiService     `json:"ziti_service"`
	Configs          []DebugConfig        `json:"configs"`
	BindPolicy       *DebugServicePolicy  `json:"bind_policy,omitempty"`
	DialPolicy       *DebugServicePolicy  `json:"dial_policy,omitempty"`
	OtherPolicies    []DebugServicePolicy `json:"other_policies,omitempty"`
	Terminators      []DebugTerminator    `json:"terminators"`
}

type DebugZitiService struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	RoleAttributes []string `json:"role_attributes"`
}

type DebugConfig struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	ConfigTypeID   string          `json:"config_type_id"`
	ConfigTypeName string          `json:"config_type_name"`
	Data           json.RawMessage `json:"data"`
}

type DebugServicePolicy struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	IdentityRoles []string `json:"identity_roles"`
	ServiceRoles  []string `json:"service_roles"`
}

type DebugTerminator struct {
	ID          string `json:"id"`
	Identity    string `json:"identity"`
	RouterID    string `json:"router_id"`
	RouterName  string `json:"router_name"`
	Precedence  string `json:"precedence"`
	Cost        int32  `json:"cost"`
	DynamicCost int32  `json:"dynamic_cost"`
	Binding     string `json:"binding"`
	Address     string `json:"address"`
}

func NewDebugHTTPServer(store ExposureStore, zitiMgmt zitimanagementv1.ZitiManagementServiceClient, token string) *DebugHTTPServer {
	return &DebugHTTPServer{store: store, zitiMgmt: zitiMgmt, token: token}
}

func (s *DebugHTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(debugExposurePathPrefix, s.handleExposure)
	return mux
}

func (s *DebugHTTPServer) handleExposure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeDebugError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.authorized(r.Header.Get(debugTokenHeader)) {
		writeDebugError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	exposureID, err := parseDebugExposureID(r.URL.Path)
	if err != nil {
		writeDebugError(w, http.StatusBadRequest, err.Error())
		return
	}
	state, err := s.debugExposureState(r.Context(), exposureID)
	if err != nil {
		writeDebugStatusError(w, err)
		return
	}
	writeDebugJSON(w, http.StatusOK, state)
}

func (s *DebugHTTPServer) authorized(value string) bool {
	return subtle.ConstantTimeCompare([]byte(value), []byte(s.token)) == 1
}

func (s *DebugHTTPServer) debugExposureState(ctx context.Context, exposureID uuid.UUID) (DebugExposureState, error) {
	exposure, err := s.store.GetExposure(ctx, exposureID)
	if err != nil {
		return DebugExposureState{}, toStatusError(err)
	}
	serviceName := exposureServiceName(exposure.ID)
	resp, err := s.zitiMgmt.DebugServiceState(ctx, &zitimanagementv1.DebugServiceStateRequest{
		ZitiServiceId:   exposure.OpenZitiServiceID,
		ZitiServiceName: serviceName,
	})
	if err != nil {
		return DebugExposureState{}, err
	}
	return toDebugExposureState(exposure, serviceName, resp)
}

func parseDebugExposureID(path string) (uuid.UUID, error) {
	value := strings.TrimPrefix(path, debugExposurePathPrefix)
	if value == "" || strings.Contains(value, "/") {
		return uuid.UUID{}, errors.New("invalid exposure id")
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.UUID{}, errors.New("invalid exposure id")
	}
	return id, nil
}

func toDebugExposureState(exposure store.Exposure, serviceName string, resp *zitimanagementv1.DebugServiceStateResponse) (DebugExposureState, error) {
	configs, err := toDebugConfigs(resp.GetConfigs())
	if err != nil {
		return DebugExposureState{}, err
	}
	state := DebugExposureState{
		ExposureID:       exposure.ID.String(),
		ServiceName:      serviceName,
		ZitiServiceID:    resp.GetZitiServiceId(),
		ZitiBindPolicyID: exposure.OpenZitiBindPolicyID,
		ZitiDialPolicyID: exposure.OpenZitiDialPolicyID,
		ZitiService: DebugZitiService{
			ID:             resp.GetZitiServiceId(),
			Name:           resp.GetZitiServiceName(),
			RoleAttributes: append([]string(nil), resp.GetRoleAttributes()...),
		},
		Configs:     configs,
		Terminators: toDebugTerminators(resp.GetTerminators()),
	}
	state.BindPolicy, state.DialPolicy, state.OtherPolicies = splitDebugPolicies(
		resp.GetServicePolicies(),
		exposure.OpenZitiBindPolicyID,
		exposure.OpenZitiDialPolicyID,
	)
	return state, nil
}

func toDebugConfigs(configs []*zitimanagementv1.DebugConfig) ([]DebugConfig, error) {
	items := make([]DebugConfig, len(configs))
	for i, config := range configs {
		data := json.RawMessage(config.GetJson())
		if len(data) == 0 {
			data = json.RawMessage("null")
		}
		if !json.Valid(data) {
			return nil, status.Error(codes.Internal, "ziti config debug json is invalid")
		}
		items[i] = DebugConfig{
			ID:             config.GetId(),
			Name:           config.GetName(),
			ConfigTypeID:   config.GetConfigTypeId(),
			ConfigTypeName: config.GetConfigTypeName(),
			Data:           data,
		}
	}
	return items, nil
}

func splitDebugPolicies(policies []*zitimanagementv1.DebugServicePolicy, bindID, dialID string) (*DebugServicePolicy, *DebugServicePolicy, []DebugServicePolicy) {
	var bindPolicy *DebugServicePolicy
	var dialPolicy *DebugServicePolicy
	otherPolicies := make([]DebugServicePolicy, 0)
	for _, policy := range policies {
		converted := toDebugPolicy(policy)
		switch policy.GetId() {
		case bindID:
			bindPolicy = &converted
		case dialID:
			dialPolicy = &converted
		default:
			otherPolicies = append(otherPolicies, converted)
		}
	}
	return bindPolicy, dialPolicy, otherPolicies
}

func toDebugPolicy(policy *zitimanagementv1.DebugServicePolicy) DebugServicePolicy {
	return DebugServicePolicy{
		ID:            policy.GetId(),
		Name:          policy.GetName(),
		Type:          policy.GetType(),
		IdentityRoles: append([]string(nil), policy.GetIdentityRoles()...),
		ServiceRoles:  append([]string(nil), policy.GetServiceRoles()...),
	}
}

func toDebugTerminators(terminators []*zitimanagementv1.DebugTerminator) []DebugTerminator {
	items := make([]DebugTerminator, len(terminators))
	for i, terminator := range terminators {
		items[i] = DebugTerminator{
			ID:          terminator.GetId(),
			Identity:    terminator.GetIdentity(),
			RouterID:    terminator.GetRouterId(),
			RouterName:  terminator.GetRouterName(),
			Precedence:  terminator.GetPrecedence(),
			Cost:        terminator.GetCost(),
			DynamicCost: terminator.GetDynamicCost(),
			Binding:     terminator.GetBinding(),
			Address:     terminator.GetAddress(),
		}
	}
	return items
}

func exposureServiceName(exposureID uuid.UUID) string {
	return "exposed-" + exposureID.String()
}

func writeDebugStatusError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	switch code {
	case codes.InvalidArgument:
		writeDebugError(w, http.StatusBadRequest, status.Convert(err).Message())
	case codes.NotFound:
		writeDebugError(w, http.StatusNotFound, status.Convert(err).Message())
	case codes.PermissionDenied, codes.Unauthenticated:
		writeDebugError(w, http.StatusForbidden, status.Convert(err).Message())
	default:
		writeDebugError(w, http.StatusInternalServerError, status.Convert(err).Message())
	}
}

func writeDebugError(w http.ResponseWriter, statusCode int, message string) {
	writeDebugJSON(w, statusCode, map[string]string{"error": message})
}

func writeDebugJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		panic(err)
	}
}
