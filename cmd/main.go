package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/kube-mirror/kube-mirror/internal/controller"
)

func main() {
	var metricsAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		`Address the metrics endpoint binds to. Use "0" to disable.`)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("setup")

	cfg, err := ctrl.GetConfig()
	if err != nil {
		log.Error(err, "unable to load kubeconfig")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Metrics: metricsserver.Options{BindAddress: metricsAddr},
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {
					Label: labels.SelectorFromSet(labels.Set{controller.LabelEnabled: "true"}),
				},
			},
		},
	})

	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := (&controller.SecretReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller", "controller", "secret")
		os.Exit(1)
	}

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
