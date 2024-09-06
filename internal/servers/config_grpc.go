package servers

import (
	"context"

	"github.com/c12s/star/internal/domain"
	"github.com/c12s/star/internal/mappers/proto"
	"github.com/c12s/star/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type starConfigServer struct {
	api.UnimplementedStarConfigServer
	configs domain.ConfigStore
}

func NewStarConfigServer(configs domain.ConfigStore) (api.StarConfigServer, error) {
	return &starConfigServer{
		configs: configs,
	}, nil
}

func (s *starConfigServer) GetStandaloneConfig(ctx context.Context, req *api.GetReq) (*api.NodeStandaloneConfig, error) {
	config, err := s.configs.GetStandalone(req.Org, req.Name, req.Version, req.Namespace)
	if err := mapError(err); err != nil {
		return nil, err
	}
	return proto.StandaloneConfigFromDomain(*config)
}

func (s *starConfigServer) GetConfigGroup(ctx context.Context, req *api.GetReq) (*api.NodeConfigGroup, error) {
	config, err := s.configs.GetGroup(req.Org, req.Name, req.Version, req.Namespace)
	if err := mapError(err); err != nil {
		return nil, err
	}
	return proto.ConfigGroupFromDomain(*config)
}

func mapError(err *domain.Error) error {
	if err == nil {
		return nil
	}
	switch err.ErrType() {
	case domain.ErrTypeDb:
		return status.Error(codes.Internal, err.Message())
	case domain.ErrTypeMarshalSS:
		return status.Error(codes.Internal, err.Message())
	case domain.ErrTypeNotFound:
		return status.Error(codes.NotFound, err.Message())
	case domain.ErrTypeVersionExists:
		return status.Error(codes.AlreadyExists, err.Message())
	case domain.ErrTypeUnauthorized:
		return status.Error(codes.PermissionDenied, err.Message())
	case domain.ErrTypeInternal:
		return status.Error(codes.Internal, err.Message())
	default:
		return status.Error(codes.Unknown, err.Message())
	}
}
