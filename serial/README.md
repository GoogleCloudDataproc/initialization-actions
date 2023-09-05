# Serial-Actions

This initialization action executes the specified initialization action paths in a serial batch


## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a cluster :

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER-NAME>`:

    ```bash
    PATH_SEPARATOR=";"
    CLUSTER_NAME=<cluster_name>
    REGION=<region>
    # See the [best practices](/README.md#how-initialization-actions-are-used) for guidance.
    INIT_ACTIONS_ROOT=gs://goog-dataproc-initialization-actions-${REGION}
    actions=(
        "${INIT_ACTIONS_ROOT}/oozie/oozie.sh"
        "${INIT_ACTIONS_ROOT}/python/pip-install.sh"
        )

    # Convert the list of actions into a semicolon separated value string
    INIT_ACTION_PATHS=$(printf "%s${PATH_SEPARATOR}" "${actions[@]}" | sed -e "s/${PATH_SEPARATOR}$//")
    
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://${INIT_ACTIONS_ROOT}/serial/serial.sh \
        --metadata "init-actions-root=${INIT_ACTIONS_ROOT} \
        --metadata "initialization-action-paths=${INIT_ACTION_PATHS}" \
        --metadata "initialization-action-paths-separator=${PATH_SEPARATOR}" \
        --metadata "PIP_PACKAGES=pandas=2.1.0"
    ```
    
    Optional arguments which can be passed as --metadata values:

    1. initialization-action-paths - a single string of hcfs paths separated by semicolon or separator as specified
    1. initialization-action-paths-separator - a single string used to delimit entries in initialization-action-paths ; default ";"
    1. init-actions-root - a single string specifying the bucket directory where
       user has cached their known good initialization actions.  If this value
       is specified, the value provided as initialization-action-paths can be
       assumed to be relative to this root directory.

## Testing serial

