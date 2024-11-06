# How to become a contributor and submit your own code

## Contributor License Agreements

We'd love to accept your patches! Before we can take them, we
have to jump a couple of legal hurdles.

Please fill out either the individual or corporate Contributor License Agreement
(CLA).

  * If you are an individual writing original source code and you're sure you
    own the intellectual property, then you'll need to sign an [individual CLA]
    (https://developers.google.com/open-source/cla/individual).
  * If you work for a company that wants to allow you to contribute your work,
    then you'll need to sign a [corporate CLA]
    (https://developers.google.com/open-source/cla/corporate).

Follow either of the two links above to access the appropriate CLA and
instructions for how to sign and return it. Once we receive it, we'll be able to
accept your pull requests.

## Contributing A Patch

1. Submit an issue describing your proposed change to the repo in question.
1. The repo owner will respond to your issue promptly.
1. If your proposed change is accepted in comments of the issue, and you haven't already done so, sign a
   Contributor License Agreement (see details above).
1. [Fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo) the desired repo, develop and test your code changes locally.
1. Raise a pull request.
1. Assign reviewers who can then trigger presubmit integration tests with comment `/gcbrun`

## Coding Practices
1. [Write small PRs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/getting-started/best-practices-for-pull-requests#write-small-prs), this helps reviewers to provide feedback and reason why something is changed. Try to keep changes < 30 lines for modifying existing init actions. 
1. [Provide context and guidance](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/getting-started/best-practices-for-pull-requests#provide-context-and-guidance) in the title/description.
1. Squash commit messages into final one while merging to remove intermediate changes, to keep commit history clean.
1. Ensure that your code adheres to the existing style in the sample to which you are contributing. Shell scripts should follow the [Google shell style guide](https://google.github.io/styleguide/shell.xml).
1. Ensure that your code has an appropriate set of unit/integration tests which all pass.

## Best Practices

The following best-practice guidelines will help ensure your initialization
actions are less likely to break from one Dataproc version to another, and
most likely to support different single-node, high-availability, and
standard cluster modes.

1. Where possible, use `apt-get install` to install from Dataproc's prebuilt
   Debian packages (built using Apache Bigtop) instead of installing from
   tarballs. The list of Dataproc packages can be found under
   `/var/lib/apt/lists/*dataproc-bigtop-repo_*Packages` on any Dataproc cluster.
1. Do not string-replace or string-grep fields out of Hadoop XML files; instead,
   use native Hadoop tooling (such as `hdfs getconf`) or `bdconfig`. `bdconfig`
   is a Python utility available on Dataproc clusters to interact with the XML
   files.
1. If it's not possible to make the additional software inherit Hadoop
   classpaths and configuration via `/etc/hadoop/conf`, then where possible
   use symlinks to necessary jarfiles and conf files instead of copying
   them into directories used by your software. This helps perserve having
   a single source of truth for jarfile versions and configuration files
   rather than letting them diverge in the face of further customization.
1. Use the `dataproc-role` metadata key to distinguish behavior between
   workers, masters, etc.
1. Do not assume node names are always a suffix on the ${CLUSTER_NAME};
   for example, do *not* assume that ${CLUSTER_NAME}-m is the HDFS namenode.
   Instead, use things like `fs.default.name` from
   `/etc/hadoop/conf/core-site.xml` to determine a default filesystem URI,
   `/etc/hadoop/conf/hdfs-site.xml` for other HDFS settings,
   `/etc/zookeeper/conf/zoo.cfg` for Zookeeper nodes, etc.
   See the [Apache Drill initialization action](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/drill/drill.sh) for examples.
1. Instead of directly launching any long-running daemon services,
   create a systemd config to ensure your daemon service automatically
   restarts on reboot or crash. See [#111](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/pull/111/files)
   and [#113](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/pull/113/files)
   for an example of integrating Jupyter with systemd.
