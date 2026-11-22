package controller

const (
	Prefix = "kube-mirror.io/"

	LabelEnabled = Prefix + "enabled"

	AnnotationTargetNamespaces = Prefix + "target-namespaces"

	AnnotationSource = Prefix + "source"

	LabelManagedBy = "app.kubernetes.io/managed-by"
	ManagedByValue = "kube-mirror"
)
