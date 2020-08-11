package main

import (
	"context"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"

	"github.com/kyma-project/kyma/components/kyma-operator/pkg/kymahelm"
	"github.com/kyma-project/kyma/components/kyma-operator/pkg/overrides"
	"github.com/sirupsen/logrus"
)

type Component struct {
	Name      string
	Namespace string
}

var components = []Component{
	Component{
		Name:      "istio-kyma-patch",
		Namespace: "istio-system",
	},
	Component{
		Name:      "knative-serving",
		Namespace: "knative-serving",
	},
	Component{
		Name:      "knative-eventing",
		Namespace: "knative-eventing",
	},
	Component{
		Name:      "dex",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "ory",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "api-gateway",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "rafter",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "service-catalog",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "service-catalog-addons",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "nats-streaming",
		Namespace: "natss",
	},
	Component{
		Name:      "core",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "cluster-users",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "permission-controller",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "apiserver-proxy",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "iam-kubeconfig-service",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "serverless",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "knative-provisioner-natss",
		Namespace: "knative-eventing",
	},
	Component{
		Name:      "event-sources",
		Namespace: "kyma-system",
	},
	Component{
		Name:      "application-connector",
		Namespace: "kyma-integration",
	},
	Component{
		Name:      "console",
		Namespace: "kyma-system",
	},
}

func main() {

	log.Println("Starting...")

	logger := logrus.New()
	logger.Out = ioutil.Discard

	start := time.Now()

	helmClient := kymahelm.NewClient(logger, 10, "secrets", false)

	// Install prerequisite components
	installComponent(helmClient, Component{Name: "cluster-essentials", Namespace: "kyma-system"})
	installComponent(helmClient, Component{Name: "testing", Namespace: "kyma-system"})
	installComponent(helmClient, Component{Name: "istio", Namespace: "istio-system"})

	log.Println("Waiting for a minute..")
	time.Sleep(1 * time.Minute)

	// Install the rest of the components
	jobChan := make(chan Component, 30)
	for _, comp := range components {
		if !enqueueJob(comp, jobChan) {
			log.Printf("Max capacity reached, component dismissed: %s", comp.Name)
		}
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go worker(ctx, &wg, helmClient, jobChan)
	}

	// to stop the workers, first close the job channel
	close(jobChan)
	wait(&wg, 10*time.Minute)
	cancel()

	duration := time.Since(start)
	log.Printf("Installation took %s", duration.String())

	releases, err := helmClient.ListReleases()
	if err != nil {
		log.Fatal(err)
	}
	if releases != nil {
		printReleases(helmClient, releases)
	}

	log.Println("Finishing...")
}

func installComponent(helmClient *kymahelm.Client, component Component) {
	componentPath := path.Join("/Users/I332735/Go/src/github.com/kyma-project/kyma/resources", component.Name)
	nn := kymahelm.NamespacedName{Name: component.Name, Namespace: component.Namespace}

	tlsCrt := "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUU4RENDQXRpZ0F3SUJBZ0lKQUpBKzlrQzZZZnZlTUEwR0NTcUdTSWIzRFFFQkN3VUFNQmN4RlRBVEJnTlYKQkFNTURDb3VhM2x0WVM1c2IyTmhiREFlRncweE9EQTNNVGd3T0RNNE1UTmFGdzB5T0RBM01UVXdPRE00TVROYQpNQmN4RlRBVEJnTlZCQU1NRENvdWEzbHRZUzVzYjJOaGJEQ0NBaUl3RFFZSktvWklodmNOQVFFQkJRQURnZ0lQCkFEQ0NBZ29DZ2dJQkFPZ05XbVROOU1ranlrUjdvQ0JGSFVqL01EcWh5bml3NEJITGo4ZTdDTFV5dFdwVHZXTkoKU1FiaFZDK3c5NkhHbU50MHZGUTR4OExUa3NNUmorcVZrdkcwKzBDTE1WQm14UjBVdnpZR09QRVRKREtsNTZkcQppMEM1Y2dnU3dkNjcveWxRZmQxc3FEVVBHM0pVZlNOSFFRWSs1SkUwaUpjZXhCQ3cvS200UXlUQXB0aEwzdEgxCm1ZRFBBQ0hUdVpsbGc2RVN4Z2RKY20rVmg5UkRvbWlON250ZjBZVG1xV1VIZXhZUkUrcTFGY0VhUHg5L2Q5QUwKZWd1WXZHTkduMVA4K1F3ZE5DKzVaMEVGa1EyS3RtalRBR09La2xJUG5NQkZubXhGSEtNZzNTa3RkYzYzSU5TNwpjSDM3dDNjb2l4N09HcDllcnRvSFZ5K2U5KzdiYnI3Z0lhWHNORVZUcjRGWXJIMlNPeHI1MjVRQUpHTys1dllQCkNYSlJJdXFNZWU3ZTg3aDRIU3JFVVdWTnhOdElBR1ZNYlRtV2F6aDJsSEFoazJjZVIzUkRaQzB3NGFOd3NRU1UKZE1yMmFCaUFobmFNSG40YnZIZnd1OHFRcjF5aVRUNG00SkltRkZTNDN2bGNETDZmVUJnNnF0U2p6QjF0WTFjTwp2Mnl1QXQzR1lKSENVMFd4R1ZEQk80T0ZncEc3aE5SRHoyRzVFMXQvRWU2VDUxdys2K295eXhib01pK09kVWdNCnBZOGlzcDdBTWhtdlZFOUdtNUhwQkpBRjYvcmE1WUNnNDh1SWtybkdZbm85eVNKcGhLU0JsMFRML01PNVFRaUoKb0hFNnV5ZkJXQjlZZ1NBdDl0MjJqU3FSWkpSdEtrbHB6bFJIRkROWTRwZlRraXlPSlFXYlM4RXJBZ01CQUFHagpQekE5TUFrR0ExVWRFd1FDTUFBd0N3WURWUjBQQkFRREFnWGdNQ01HQTFVZEVRUWNNQnFDQ210NWJXRXViRzlqCllXeUNEQ291YTNsdFlTNXNiMk5oYkRBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQWdFQUlYYTlwenlyQ2dzMTRTOHUKZVFZdkorNEFzUE9uT1RGcExkaVl5UkVyNXdyNmJuMXUvMjZxc2FKckpxbkkyRk16SmdEQVRwZEtjbXRHYjBUOQp3S2wrYUJHcFFKcThrUWJwakVGTHhaWDJzaUNrRG82WittaUcrRjRKMHpKa3BKK0JHMS92eGZKbk0zK1ptdXQ5Ck9RV2ZjYTN3UHlhTWRDbGIyZjQwYlRFaFo5Mk9kcWlQMzFMbDlHWExSZmhaNTNsUzF0QWdvUGZoR25NbFY4b2MKWmxuSUROK25wS0Nma2tXUDJZUjlRLy9pa01tM01YRm9RSFppaVJseVZHSGFKZWRLMmNOQzlUYk4xNDFTaWZHZQo3V2FsQVBNcWNOQ3F3YStnN2RFSmR3ZjlRMklJTml0SjlDUVprT1dUZElYY2VHK2lZWWUrQXpmK1NkaHBocVdPCllFcDF6ek40dXI5U2VxU3NSaU9WY0RzVUFSa1M0clgrb0Vzb2hHL1Q5OTcrSDhjR2gzczl6TE84emtwRXZKSmEKS05QT0N5ODhVeEFOV2RRejFLMXRKVVQ2c3hkd0FEcXRJQnNPemhYVjlybDRRNStlZExlcmZPcUtCbUFRMUY5Swo2L1l0ZlNyY0JpeXZEU24wdFJ3OHJLRFVQU1hFNDFldXArOURNeThLVGl6T0RPTXVMSnR2dkJrTEFpNGNYQjVBCjQxMjBEdHdZQXNyNzNZYVl2SW8rWjV2OGZ4TjF3M3IwYS9KOVhZQlg3S3p1OFl4MnNUNWtWM2dNTHFCTXBaa3gKY29FTjNSandDMmV4VHl6dGc1ak1ZN2U4VFJ4OFFTeUxkK0pBd2t1Tm01NlNkcHFHNTE3cktJYkVMNDZzbkd0UgpCYUVOK01GeXNqdDU3ejhKQXJDMzhBMFN5dTQ9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
	tlsKey := "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUpRd0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQ1Mwd2dna3BBZ0VBQW9JQ0FRRG9EVnBremZUSkk4cEUKZTZBZ1JSMUkvekE2b2NwNHNPQVJ5NC9IdXdpMU1yVnFVNzFqU1VrRzRWUXZzUGVoeHBqYmRMeFVPTWZDMDVMRApFWS9xbFpMeHRQdEFpekZRWnNVZEZMODJCamp4RXlReXBlZW5hb3RBdVhJSUVzSGV1LzhwVUgzZGJLZzFEeHR5ClZIMGpSMEVHUHVTUk5JaVhIc1FRc1B5cHVFTWt3S2JZUzk3UjlabUF6d0FoMDdtWlpZT2hFc1lIU1hKdmxZZlUKUTZKb2plNTdYOUdFNXFsbEIzc1dFUlBxdFJYQkdqOGZmM2ZRQzNvTG1MeGpScDlUL1BrTUhUUXZ1V2RCQlpFTgppclpvMHdCamlwSlNENXpBUlo1c1JSeWpJTjBwTFhYT3R5RFV1M0I5KzdkM0tJc2V6aHFmWHE3YUIxY3ZudmZ1CjIyNis0Q0dsN0RSRlU2K0JXS3g5a2pzYStkdVVBQ1JqdnViMkR3bHlVU0xxakhudTN2TzRlQjBxeEZGbFRjVGIKU0FCbFRHMDVsbXM0ZHBSd0laTm5Ia2QwUTJRdE1PR2pjTEVFbEhUSzltZ1lnSVoyakI1K0c3eDM4THZLa0s5YwpvazArSnVDU0poUlV1Tjc1WEF5K24xQVlPcXJVbzh3ZGJXTlhEcjlzcmdMZHhtQ1J3bE5Gc1JsUXdUdURoWUtSCnU0VFVRODlodVJOYmZ4SHVrK2RjUHV2cU1zc1c2REl2am5WSURLV1BJcktld0RJWnIxUlBScHVSNlFTUUJldjYKMnVXQW9PUExpSks1eG1KNlBja2lhWVNrZ1pkRXkvekR1VUVJaWFCeE9yc253VmdmV0lFZ0xmYmR0bzBxa1dTVQpiU3BKYWM1VVJ4UXpXT0tYMDVJc2ppVUZtMHZCS3dJREFRQUJBb0lDQUZDN3ZKUlB0M2QzVlRyb1MvaU9NemNmCldaYzhqT1hhbThwMUtRdlRQWjlWQ2hyNUVXNEdwRHFaa0tHYkR6eWdqTFBsZEZSVkFPTCtteFAwK3o0aFZlTjAKRk9vS3cxaDJ1T042UVdBNVgvdzNyYU5WWnpndThFM1BkeVhwNkx0bWFzcmo3elpuUkVwWmZESVZ4UWZPRllobgp2enZwckEvdnEwVW5YbkJwNUNwWVFIUUdTWHFBMlN3Z1dLcHNNQ2wzVVFsc0w2dC9XU29MT3h1VmdGNmg2clBQCnpXUlFuK1MvYW9wdDNLRU82WWVxYXdXNVltVG1hVXE1aytseU82S0w0OVhjSHpqdlowWU8rcjFjWWtRc0RQbVUKejMxdll4amQzOVZKWWtJNi85Y0Fzdmo5YTVXM3ROYVFDZStTRW56Z05oRDJieHo1NnRKdG0xTGwweXpqYTdEVgp0OW9aQVd6WURMOTVnMDFZWVlQMG51c3A4WWlNOXd0OUtLc2NkcUd3RFN5WHNrWFBrcWFLVjNVcHJYdmhFbFVaCkErMmtjcm9VaDNGbEV4bGpwUmtKVkhOZXJ3NHRLRi9oYTFWRjZPdE10eTVQcXV0N0dGQmIvamtWeUg5cnpueWUKTXQyTWVyTTVPazMwd1NuTThISUdTUXpxYlplekJEZlNaUzRzcWdZZnBIMlhtMEs0SjgrRUowQ2hhMXZVSmVNMAoyZ284d00vaHljdmtqTEgxSmM3OEhpaVBTQ01udkpHemUxc2tWdmtRRFhBSFdldzBTUHpUSTZHYjZCb0Y4aVNECm0wZjR2azNoV3NlUWZBaXVZSnlUeUZXNmRhOGE1K2lpSDN4cVRsUUN1MDN1Nmo0U0l4aThJZlNmd0YwQTBldVAKNGtzalZTZVZyT3ExUnlvNUtpR3hBb0lCQVFEOWZtYnl6aW9QdVhRYzl0QXBxMUpSMzErQzlCdFFzcDg5WkZkSQpQaU5xaTJ3NVlVcTA0OFM2Z3VBb3JGOHNObUI3QjhWa1JlclowQ3hub2NHY0tleWdTYWsvME5qVElndk5weGJwCnBGbkFnRjlmbW1oTEl2SlF2REo2Q0ZidDRCQlZIdkJEWlYyQnZqK0k3NUxkK01jN2RPVDdFek1FRjBXcUdzY2MKTUpyNjRXQi9UMkF5dWR1YXlRT2NobmJFQ25FUmdRcHFlbG54MVBraytqbGNvYUs5QjFYUStVOUgzOHppM0FYNApENUxMY0Nhem9YYWlvS0swckNlNE5Ga2hOVXd0TFV0QXhSTXk2aksyZUZudWczUFRlY3N1WktNMElITktqZ0dCCnpGanZVb2tMcFVFb3BJa3FHM09yc0xmanpHZW9jaVFPUXNEdzlUb3lXL0FSOFhmWkFvSUJBUURxV0s2TThVN3EKUXJPeTYzNnpEZlBaZ2ljeXlsUWVoOFdMclBlbW5NeWdQYzR4eWoybnMrTmVRSnNEUmtPT2tWY250SEFYaTcwWgoyT0NCV3dwZHJuTXlSc3RIMU05bjdFNU5TVHZlZDlkU3YxUzRBb3NzS1hDSmgyUHBjYjV0OE9nL3ZGTlNYUlUyClk2aUorWTdOcDBZNDNxSlJOVnlRemd3YmFzaEpiUVdkVFFoVEVhdEVRS2JsUlZSblhlQWRjOXlhNUpHbkRpaTkKbFQrRWEzdFpvN1dha05oeHJkYjVuTkZ3a0xoNEs3ZkFtT3ZzMVBMQWx0SUZqeURCeDEvY1ZHblpDUDBVQmJqZgpkU2FueXBBdVRuMzd1VUwrcXpPVDlYWEZENllGT0x0NWV4d3RxdnUwSzZCNjcvajFFTlJDRk45RnMzWlV5RFFXClZUaDcybFhWU3NLakFvSUJBUUR6ZE5pdXpTNDhWK0thaHJpNXJGNnRYeGkrRG0vRmV5ZlFzSFBiWUVKbmEyd1AKVjgrR0YxS3p4a28vQmYySjJ0ZWlrWDRVcGNtK1UxNnlVUG8vWDB4eFRRMk55cWpUYmRsa005dWZuVWJOeVB6UQpOdDEvZkJxNVMyWTNLWmREY25SOUsrK1k2dHQ1Wmh4akNhUkdKMDVCWGkwa3JmWExNZ2FvTG51WUtWNVBJUEdxCms3TlNSSW9UQ0llOVpxN2Q3U0ZXckZZeW1UdVZOUFByZlo1bHhwOGphTTRVbTd4MnpReGJ2UERHb3o1YXdHV0wKRThGNncwaEF1UzZValVJazBLbE9vamVxQnh3L1JBcGNrUTNlTXNXbEQwNENTb2tyNFJhWlBmVllrY2ZBWWNaWgpOdWR6ZjBKMC9GU0ZTbjN4L0RoNTROV2NGS1IxUnpBVGVaVUJ4cVZSQW9JQkFRRFlDNmZvVWpOQnJ2ckNFVzkrCkhYZlk1Ni9CbUZ4U3hUTHU0U2h6VncwakViZTltVWljQ2pDc1hQMUwySVJCdEdaWU9YWTVqdDlvSzlSV0RSdVMKWUZqZFdmemduU1lWRmZyZUw0emRQVGlxbGEvQjhMNWptVlNoeGNycmxheE02Uk1FWjFlZGtDa1ZPbTFQdmwzVAo1TW5OZGhySXFWeE1OMWxjRVdiU29vclJpUW9Lb3poMHRQSG9YckZBbG9BZVJ3bHpWeE9jb21ZVzJiaDBHUzdmCjVoaHZoZWUxYmVISnY3UXFoWkU3WUhxSU9iTVBaUWJqWEdnRkxmMnlDRitzM2Jtem1DRFJTN0V6ZVdxSXVDdVMKTlZUYU0rSzZyQlRoN0NLRjZUWlNqQW55SmZoRmRlT1ZKNzlNZDEzYWVJaG0zNTB6UWc3dWZKL2drdkorNUR2TApacC9uQW9JQkFBVlg4WHpFTzdMVk1sbENKZFVUdURTdXNPcDNrQVlFZ2dZNFFRM3FNTlcxRnl5WEM3WjBGOWJFCmtTSEhkalJtU2RUbFZueGN2UW1KTS9WL2tJanpNUHhFT3NCS1BVVkR3N3BhOHdiejlGcTRPOCtJb3lqN1ZXclcKMmExL1FNWXlzSGlpTlBzNy8vWUtvMy9rdkhCWUY2SnNkenkyQkVSTkQ0aTlVOWhDN0RqcGxKR3BSNktMTVBsegpNWFJ3VjVTM2V3cnBXZVcxQW5ONC9EKy9zUGlNQTNnS0swSlBFdGVBV1dndEZHTnNBSkJnaFBoUExxQi9CcDUvCkhOeC96M0w0MWtqRnpqOHNWaHMrVDRZYlhiaGF2R2xxc2h5ZldQbnRhV1VOMG15MjU1RFdQUDhWa24yeFNlV2kKVm1hVW5TSDBTZ2tlUENMRnlra25yQzgxU2pXZkRBMD0KLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLQo="

	overrides := overrides.Map{
		"global": map[string]interface{}{
			"isLocalEnv": true,
			"domainName": "kyma.local",
			"minikubeIP": "192.168.64.3",
			"tlsCrt":     tlsCrt,
			"tlsKey":     tlsKey,
			"ingress": map[string]interface{}{
				"domainName": "kyma.local",
				"tlsCrt":     tlsCrt,
				"tlsKey":     tlsKey,
			},
			"environment": map[string]interface{}{
				"gardener": false,
			},
		},
	}

	_, err := helmClient.InstallRelease(componentPath, nn, overrides)
	if err != nil {
		log.Printf("Component installation failed! Component: %s, Error: %v", component.Name, err)
	} else {
		log.Printf("Installation successful! Component: %s", component.Name)
	}
}

func printReleases(helmClient *kymahelm.Client, releases []*kymahelm.Release) {
	log.Println("Listing installed releases...")

	for _, release := range releases {
		statusCode := release.Status
		_, err := helmClient.ReleaseDeployedRevision(kymahelm.NamespacedName{Namespace: release.Namespace, Name: release.Name})
		if err != nil {
			log.Printf("Helm error: " + err.Error())
		}

		log.Printf("%s status: %s", release.Name, statusCode)
	}
}

func enqueueJob(job Component, jobChan chan<- Component) bool {
	select {
	case jobChan <- job:
		return true
	default:
		return false
	}
}

func wait(wg *sync.WaitGroup, timeout time.Duration) bool {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		log.Println("Timeout occurred!")
		return false
	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, helmClient *kymahelm.Client, jobChan <-chan Component) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case job, ok := <-jobChan:
			if ctx.Err() != nil || !ok {
				return
			}
			if ok {
				installComponent(helmClient, job)
			}
		}
	}
}
