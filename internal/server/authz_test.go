package server

import (
	"context"
	"errors"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockAuthz struct {
	check func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error)
}

func (m *mockAuthz) Check(ctx context.Context, req *authorizationv1.CheckRequest, _ ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	if m.check == nil {
		return nil, errors.New("not implemented")
	}
	return m.check(ctx, req)
}

func (m *mockAuthz) BatchCheck(context.Context, *authorizationv1.BatchCheckRequest, ...grpc.CallOption) (*authorizationv1.BatchCheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockAuthz) Write(context.Context, *authorizationv1.WriteRequest, ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockAuthz) Read(context.Context, *authorizationv1.ReadRequest, ...grpc.CallOption) (*authorizationv1.ReadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockAuthz) ListObjects(context.Context, *authorizationv1.ListObjectsRequest, ...grpc.CallOption) (*authorizationv1.ListObjectsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockAuthz) ListUsers(context.Context, *authorizationv1.ListUsersRequest, ...grpc.CallOption) (*authorizationv1.ListUsersResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
