package docker

import (
	"bufio"
	"context"
	"errors"
	"github.com/docker/docker/api/types/mount"
	"io"
	"strings"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/golang/mock/gomock"
	mock_docker "github.com/kyma-incubator/hydroform/function/pkg/docker/automock"
	"github.com/stretchr/testify/require"
)

type fakeReader struct {
}

func newReader() io.Reader {
	return &fakeReader{}
}

func (fr *fakeReader) Read(_ []byte) (n int, err error) {
	return 0, errors.New("reading error")
}

func TestFollowRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	id := "test-id"

	t.Run("should follow buffer", func(t *testing.T) {
		reader := bufio.NewReader(strings.NewReader("1\n2\n2\n3\n4\n"))
		conn := mock_docker.NewMockConn(ctrl)
		conn.EXPECT().Close().Times(1)

		mock := mock_docker.NewMockContainerClient(ctrl)
		mock.EXPECT().ContainerAttach(ctx, id, types.ContainerAttachOptions{
			Stdout: true, Stderr: true, Stream: true,
		}).Return(types.HijackedResponse{Reader: reader, Conn: conn}, nil).Times(1)

		counter := 0
		log := func(i ...interface{}) { counter++ }

		err := FollowRun(ctx, mock, id, log)

		require.Equal(t, nil, err)
		require.Equal(t, 5, counter)
	})

	t.Run("should return error during read from buffer", func(t *testing.T) {
		reader := bufio.NewReader(newReader())
		conn := mock_docker.NewMockConn(ctrl)
		conn.EXPECT().Close().Times(1)

		mock := mock_docker.NewMockContainerClient(ctrl)
		mock.EXPECT().ContainerAttach(ctx, id, types.ContainerAttachOptions{
			Stdout: true, Stderr: true, Stream: true,
		}).Return(types.HijackedResponse{Reader: reader, Conn: conn}, nil).Times(1)

		counter := 0
		log := func(i ...interface{}) { counter++ }

		err := FollowRun(ctx, mock, id, log)

		require.Equal(t, errors.New("reading error"), err)
		require.Equal(t, 0, counter)
	})

	t.Run("should return error during container attach", func(t *testing.T) {
		mock := mock_docker.NewMockContainerClient(ctrl)
		mock.EXPECT().ContainerAttach(ctx, id, types.ContainerAttachOptions{
			Stdout: true, Stderr: true, Stream: true,
		}).Return(types.HijackedResponse{}, errors.New("attach: error")).Times(1)

		counter := 0
		log := func(i ...interface{}) { counter++ }

		err := FollowRun(ctx, mock, id, log)

		require.Equal(t, errors.New("attach: error"), err)
		require.Equal(t, 0, counter)
	})
}

func TestRunContainer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	id := "test-id"

	type args struct {
		c    ContainerClient
		ctx  context.Context
		opts RunOpts
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "should run container and return nil",
			args: args{
				c: func() ContainerClient {
					mock := mock_docker.NewMockContainerClient(ctrl)

					mock.EXPECT().ContainerCreate(ctx, gomock.Any(), gomock.Any(),
						gomock.Nil(), gomock.Nil(), gomock.Any()).
						Return(container.ContainerCreateCreatedBody{ID: id}, nil).Times(1)

					mock.EXPECT().ContainerStart(ctx, id, types.ContainerStartOptions{}).
						Return(nil).Times(1)

					return mock
				}(),
				ctx: ctx,
			},
			want:    id,
			wantErr: false,
		},
		{
			name: "should return an error during creating a container",
			args: args{
				c: func() ContainerClient {
					mock := mock_docker.NewMockContainerClient(ctrl)

					mock.EXPECT().ContainerCreate(ctx, gomock.Any(), gomock.Any(),
						gomock.Nil(), gomock.Nil(), gomock.Any()).
						Return(container.ContainerCreateCreatedBody{}, errors.New("create: error")).Times(1)

					return mock
				}(),
				ctx: ctx,
			},
			wantErr: true,
		},
		{
			name: "should run container and return nil",
			args: args{
				c: func() ContainerClient {
					mock := mock_docker.NewMockContainerClient(ctrl)

					mock.EXPECT().ContainerCreate(ctx, gomock.Any(), gomock.Any(),
						gomock.Nil(), gomock.Nil(), gomock.Any()).
						Return(container.ContainerCreateCreatedBody{ID: id}, nil).Times(1)

					mock.EXPECT().ContainerStart(ctx, id, types.ContainerStartOptions{}).
						Return(errors.New("start: error")).Times(1)

					return mock
				}(),
				ctx: ctx,
			},
			wantErr: true,
		},
		{
			name: "should run a container with right options and return nil",
			args: args{
				c: func() ContainerClient {
					mock := mock_docker.NewMockContainerClient(ctrl)

					mock.EXPECT().ContainerCreate(ctx, &container.Config{
						Env: []string{"env1=test1", "env2=test2"},
						ExposedPorts: map[nat.Port]struct{}{
							"8080": {},
							"9229": {},
						},
						Image: "test-iname",
						Shell: []string{},
					},
						&container.HostConfig{
							PortBindings: nat.PortMap{
								"8080": []nat.PortBinding{{HostPort: "6262"}},
								"9229": []nat.PortBinding{{HostPort: "9229"}},
							},
							AutoRemove: true,
							Mounts: []mount.Mount{
								{
									Type:   mount.TypeBind,
									Source: "",
									Target: "/kubeless",
								},
							},
						},
						gomock.Nil(), gomock.Nil(), "test-cname").
						Return(container.ContainerCreateCreatedBody{ID: id}, nil).Times(1)

					mock.EXPECT().ContainerStart(ctx, id, types.ContainerStartOptions{}).
						Return(nil).Times(1)

					return mock
				}(),
				ctx: ctx,
				opts: RunOpts{
					Ports: map[string]string{
						"8080": "6262",
						"9229": "9229",
					},
					Envs:          []string{"env1=test1", "env2=test2"},
					ContainerName: "test-cname",
					ImageName:     "test-iname",
					Commands:      []string{},
				},
			},
			want:    id,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RunContainer(tt.args.ctx, tt.args.c, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("RunContainer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RunContainer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Run ", func(t *testing.T) {
		id := "1"
		counter := 0
		ctx := context.Background()

		mock := mock_docker.NewMockContainerClient(ctrl)
		mock.EXPECT().ContainerStop(ctx, id, nil).
			Return(nil).Times(1)

		f := Stop(ctx, mock, id, func(i ...interface{}) {
			counter++
		})

		f()

		require.Equal(t, 1, counter)
	})
}
