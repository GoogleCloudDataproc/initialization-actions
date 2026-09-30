# NFS Initialization Action

This initialization action sets up an NFS server on the primary -m node and NFS clients on the worker nodes of a Dataproc cluster. It supports both standard and Kerberized clusters.

## Features

-   Installs `nfs-kernel-server` on the -m node.
-   Installs `nfs-common` and `rpcbind` on all nodes.
-   Exports a directory from the -m node (default: `/srv/nfs`).
-   Mounts the NFS share on worker nodes using systemd automount (default: `/srv/nfs`).
-   **Kerberos Support:** If the cluster has Kerberos enabled (`/etc/krb5.conf` exists):
    -   Enforces `sec=krb5p` for strong authentication, integrity, and encryption.
    -   Creates the `nfs/<hostname>@REALM` service principal and keytab on the -m node.
    -   Provisions user principals and keytabs based on `nfs-kerberos-users` metadata (see below).
    -   Restarts `rpc-gssd` on workers with an override to not require `/etc/krb5.keytab` on clients.
-   **Standard Clusters:** Uses `sec=sys` if Kerberos is not enabled.
-   **Security:** Enforces `root_squash` on the NFS export.
-   **Static Ports:** Configures static ports for `rpc.statd` (default: 32765) and `rpc.mountd` (default: 32767) on the server using systemd drop-in files to override default service settings.

## Usage

1.  Upload the `nfs.sh` script to a GCS bucket.
2.  When creating your Dataproc cluster, specify the initialization action:

    ```bash
    gcloud dataproc clusters create my-cluster \
      --region <region> \
      --initialization-actions gs://<your-bucket>/nfs/nfs.sh \
      [--enable-kerberos] \
      --metadata enable-oslogin=TRUE \
      [--metadata nfs-kerberos-users=user1,user2] \
      # ... other cluster flags
    ```

    *   `--metadata enable-oslogin=TRUE` is REQUIRED.
    *   Include `--enable-kerberos` if you are setting up a Kerberized cluster.
    *   If using Kerberos and you want per-user home directories or access, provide the `nfs-kerberos-users` metadata with a comma-separated list of OS Login usernames. The script will create matching Kerberos principals and keytabs in `/home/<user>/.krb5/<user>.keytab`.

## Metadata Parameters

This initialization action supports the following metadata attributes to customize its behavior:

*   `nfs-export-dir`: The directory on the -m node to export as the NFS share. Default: `/srv/nfs`
*   `nfs-mount-dir`: The path on worker nodes where the NFS share will be mounted. Default: `/srv/nfs`
*   `nfs-statd-port`: The static port to use for `rpc.statd`. Default: `32765`
*   `nfs-mountd-port`: The static port to use for `rpc.mountd` on the -m node. Default: `32767`
*   `nfs-export-options`: Basic NFS export options (applied before security options). Default: `rw,sync,no_subtree_check`
*   `nfs-client-options`: Basic NFS client mount options (applied before security options). Default: `nfsvers=4,rw,hard,intr,timeo=600,retrans=3`
*   `nfs-kerberos-users`: A comma-separated string of usernames. If Kerberos is enabled, principals and keytabs will be created for these users. Keytabs are placed in `/home/<user>/.krb5/<user>.keytab`. Default: ""
*   `nfs-kerberos-config-file`: Path to the Kerberos configuration file (e.g., krb5.conf). Default: `/etc/krb5.conf`

## Firewall Configuration

To allow worker nodes to communicate with the NFS server on the -m node, ensure your VPC firewall rules permit the following ingress traffic to the -m node(s) from the worker nodes (or the cluster's network range):

-   **TCP & UDP Port 111:** For `rpcbind`
-   **TCP Port 2049:** For NFS
-   **TCP & UDP Port 32765:** For `rpc.statd` (static port)
-   **TCP & UDP Port 32767:** For `rpc.mountd` (static port)

The `dataproc-repro` tooling's default firewall rules (`*-default-allow-internal-in`) should cover this, as they allow all traffic within the subnet.

## Important Considerations

-   **Worker Node Keytabs:** For Kerberos mounts (`sec=krb5p`) to work from worker nodes, each worker MUST have a valid `/etc/krb5.keytab` containing its unique `host/<worker-fqdn>@REALM` principal. This should be provisioned by the Dataproc cluster creation process when `--enable-kerberos` is used. If this file is missing on workers, Kerberized mounts will fail.
-   **JupyterLab Root Vulnerability (b/481498418):** Currently, JupyterLab runs as root on the -m node, potentially bypassing NFS security for users with Jupyter access.
-   **Jupyter Notebook Kerberos:** Automatic per-user Kerberos ticket generation within Jupyter notebooks is NOT implemented.
-   **HA:** This setup is not High Availability.

See the full architecture details in the `architecture/` directory.