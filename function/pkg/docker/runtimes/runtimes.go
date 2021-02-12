package runtimes

import (
	"fmt"

	"github.com/kyma-incubator/hydroform/function/pkg/resources/types"
)

const ServerPort = "8080"

func Dockerfile(runtime types.Runtime) string {
	switch runtime {
	case types.Nodejs12:
		return Nodejs12Dockerfile
	case types.Nodejs10:
		return Nodejs10Dockerfile
	case types.Python38:
		return Python38Dockerfile
	default:
		return Nodejs12Dockerfile
	}
}

func ContainerEnvs(runtime types.Runtime, debug bool) []string {
	return append([]string{
		fmt.Sprintf("FUNC_RUNTIME=%s", runtime),
		"FUNC_HANDLER=main",
		"MOD_NAME=handler",
		"FUNC_PORT=8080",
	}, runtimeEnvs(runtime, debug)...)
}

func runtimeEnvs(runtime types.Runtime, debug bool) []string {
	switch runtime {
	case types.Nodejs12:
		envs := []string{Nodejs12Path}
		if debug {
			envs = append(envs, fmt.Sprintf("NODE_OPTIONS=%s", Nodejs12DebugOption))
		}
		return envs
	case types.Nodejs10:
		envs := []string{Nodejs10Path}
		if debug {
			envs = append(envs, fmt.Sprintf("NODE_OPTIONS=%s", Nodejs10DebugOption))
		}
		return envs
	case types.Python38:
		envs := []string{Python38Path}
		// TODO
		//if debug { }
		return envs
	default:
		envs := []string{Nodejs12Path}
		if debug {
			envs = append(envs, fmt.Sprintf("NODE_OPTIONS=%s", Nodejs12DebugOption))
		}
		return envs
	}
}

func RuntimeDebugPort(runtime types.Runtime) string {
	switch runtime {
	case types.Nodejs12:
		return Nodejs12DebugEndpoint
	case types.Nodejs10:
		return Nodejs10DebugEndpoint
	case types.Python38:
		return Python38DebugEndpoint
	default:
		return Nodejs12DebugEndpoint
	}
}

func ContainerCommands(runtime types.Runtime) []string {
	switch runtime {
	case types.Nodejs12:
		return []string{"/kubeless-npm-install.sh", "node kubeless.js"}
	case types.Nodejs10:
		return []string{"/kubeless-npm-install.sh", "node kubeless.js"}
	case types.Python38:
		return []string{"", ""} //TODO
	default:
		return []string{"/kubeless-npm-install.sh", "node kubeless.js"}
	}
}

func ContainerImages(runtime types.Runtime) string {
	switch runtime {
	case types.Nodejs12:
		return "eu.gcr.io/kyma-project/function-runtime-nodejs12:cc7dd53f"
	case types.Nodejs10:
		return "eu.gcr.io/kyma-project/function-runtime-nodejs10:cc7dd53f"
	case types.Python38:
		return "eu.gcr.io/kyma-project/function-runtime-python38:cc7dd53f"
	default:
		return "eu.gcr.io/kyma-project/function-runtime-nodejs12:cc7dd53f"
	}
}
