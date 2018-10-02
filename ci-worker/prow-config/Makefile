get-cluster-credentials:
	gcloud container clusters get-credentials "$(CLUSTER)" --project="$(PROJECT)" --zone="$(ZONE)"

update-plugins: get-cluster-credentials
	kubectl create configmap plugins --from-file=plugins.yaml=plugins.yaml --dry-run -o yaml | kubectl replace configmap plugins -f -

update-config: get-cluster-credentials
	kubectl create configmap config --from-file=config.yaml=config.yaml --dry-run -o yaml | kubectl replace configmap config -f -
