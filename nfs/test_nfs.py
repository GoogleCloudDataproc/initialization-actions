import os
import uuid
import time
from absl.testing import absltest
from integration_tests.dataproc_test_case import DataprocTestCase

class NFSInitActionTestCase(DataprocTestCase):
    COMPONENT = "nfs"
    INIT_ACTIONS = ["nfs/nfs.sh"]
    TEST_USER = "testnfsuser"
    NFS_MOUNT = "/srv/nfs"

    def _assert_instance_command(self, instance, command, assert_success=True, timeout_s=60):
        ret_code, stdout, stderr = self.RunSshCommand(instance, command, timeout=timeout_s)
        if assert_success:
            self.assertEqual(ret_code, 0, "Command failed on {}: {}
STDOUT:
{}
STDERR:
{}".format(
                instance, command, stdout, stderr))
        return ret_code, stdout, stderr

    def _create_test_user(self, instance):
        # Create user on the instance
        self._assert_instance_command(instance, f"sudo useradd -m {self.TEST_USER} || echo 'User {self.TEST_USER} already exists'")

    def _create_kerberos_principal(self, instance, user):
        master_name = self.getClusterName() + "-m"
        princ = f"{user}"
        kadmin_cmd = f"sudo kadmin.local -q 'addprinc -randkey {princ}'"
        self._assert_instance_command(master_name, kadmin_cmd)

    def _get_test_user_keytab(self, instance):
        master_name = self.getClusterName() + "-m"
        user = self.TEST_USER
        princ = f"{user}"
        keytab_path = f"/home/{user}/{user}.keytab"
        remote_keytab_path = f"/tmp/{user}.keytab.{uuid.uuid4()}"

        ktadd_cmd = f"sudo kadmin.local -q 'ktadd -k {remote_keytab_path} {princ}'"
        chown_cmd = f"sudo chown {user}:{user} {remote_keytab_path}"
        chmod_cmd = f"sudo chmod 600 {remote_keytab_path}"

        self._assert_instance_command(master_name, ktadd_cmd)
        self._assert_instance_command(master_name, f"sudo chown root:root {remote_keytab_path}") # So we can scp

        self.RunScpCommand(master_name, remote_keytab_path, instance, keytab_path, self.getMsTimeout())
        self._assert_instance_command(master_name, f"sudo rm -f {remote_keytab_path}")

        self._assert_instance_command(instance, f"sudo chown {user}:{user} {keytab_path}")
        self._assert_instance_command(instance, f"sudo chmod 600 {keytab_path}")

    def _kinit_test_user(self, instance):
        keytab_path = f"/home/{self.TEST_USER}/{self.TEST_USER}.keytab"
        kinit_cmd = f"sudo -u {self.TEST_USER} kinit -kt {keytab_path} {self.TEST_USER}"
        self._assert_instance_command(instance, kinit_cmd)
        self._assert_instance_command(instance, f"sudo -u {self.TEST_USER} klist")

    def _verify_nfs_mount(self, instance):
        self._assert_instance_command(instance, f"mountpoint -q {self.NFS_MOUNT}")
        # Verify mount options
        mtab_out, _ = self._assert_instance_command(instance, f"grep ' {self.NFS_MOUNT} ' /etc/mtab")
        is_kerberos = self.enable_kerberos
        if is_kerberos:
            self.assertIn("sec=krb5p", mtab_out)
        else:
            self.assertIn("sec=sys", mtab_out)

    def _test_nfs_access(self, instance, user, should_have_access, test_file_suffix=""):
        test_file = f"test-file-{uuid.uuid4()}{test_file_suffix}.txt"
        file_path = os.path.join(self.NFS_MOUNT, test_file)
        write_content = f"hello from {user} on {instance} at $(date)"
        write_cmd = f"echo '{write_content}' > {file_path}"
        read_cmd = f"cat {file_path}"
        rm_cmd = f"rm -f {file_path}"

        run_as_user = f"sudo -u {user}"

        if should_have_access:
            self._assert_instance_command(instance, f"{run_as_user} {write_cmd}")
            stdout, _ = self._assert_instance_command(instance, f"{run_as_user} {read_cmd}")
            self.assertIn(write_content, stdout)
            self._assert_instance_command(instance, f"{run_as_user} ls -l {file_path}") # Check ownership
            self._assert_instance_command(instance, f"{run_as_user} {rm_cmd}")
        else:
            ret_code, _, stderr = self._assert_instance_command(instance, f"{run_as_user} {write_cmd}", assert_success=False)
            self.assertNotEqual(ret_code, 0)
            self.assertIn("Permission denied", stderr)

    def verify_nfs_setup(self):
        master_name = self.getClusterName() + "-m"
        self._assert_instance_command(master_name, "systemctl is-active nfs-server")
        self._assert_instance_command(master_name, "systemctl is-active rpcbind")
        if self.enable_kerberos:
            self._assert_instance_command(master_name, "systemctl is-active rpc-gssd || systemctl is-active gssd")

        # Verify static ports
        rpcinfo_m, _ = self._assert_instance_command(master_name, "rpcinfo -p")
        self.assertIn(" 32767 ", rpcinfo_m)  # MOUNTD
        self.assertIn(" 32765 ", rpcinfo_m)  # STATD

        for instance in self.getCluster().GetAllHelperInstances():
            self._verify_nfs_mount(instance)
            self._assert_instance_command(instance, "systemctl is-active rpcbind")
            if self.enable_kerberos:
                self._assert_instance_command(instance, "systemctl is-active rpc-gssd || systemctl is-active gssd")

            if instance != master_name:
                rpcinfo_w, _ = self._assert_instance_command(instance, "rpcinfo -p")
                self.assertIn(" 32765 ", rpcinfo_w)  # STATD

            self._create_test_user(instance)

            if self.enable_kerberos:
                self._create_kerberos_principal(instance, self.TEST_USER)
                self._get_test_user_keytab(instance)

                # Before kinit, access should be denied
                self._test_nfs_access(instance, self.TEST_USER, should_have_access=False, test_file_suffix="_before_kinit")

                # After kinit, access should be allowed
                self._kinit_test_user(instance)
                self._test_nfs_access(instance, self.TEST_USER, should_have_access=True, test_file_suffix="_after_kinit")
            else:
                # Without Kerberos, standard Unix permissions apply
                self._test_nfs_access(instance, self.TEST_USER, should_have_access=True, test_file_suffix="_no_kerberos")

            # Root on worker should be squashed
            if instance != master_name:
                 ret_code, _, stderr = self._assert_instance_command(instance, f"sudo touch {self.NFS_MOUNT}/root_test.txt", assert_success=False)
                 self.assertNotEqual(ret_code, 0)
                 self.assertIn("Permission denied", stderr)

    def test_nfs_standard(self):
        self.enable_kerberos = False
        self.createCluster(
            "STANDARD",
            self.INIT_ACTIONS,
            timeout_in_minutes=30)
        self.verify_nfs_setup()

    def test_nfs_kerberos(self):
        self.enable_kerberos = True
        self.createCluster(
            "KERBEROS",
            self.INIT_ACTIONS,
            timeout_in_minutes=30,
            scopes=['https://www.googleapis.com/auth/cloud-platform'] # Added scope for kadmin
        )
        self.verify_nfs_setup()

if __name__ == "__main__":
    absltest.main()
