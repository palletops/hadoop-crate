## 0.1.6

- fix regression where deploying on vmfest would not create /etc/hosts

- Update .gitignore to remove generated files.

- Filter out terminated nodes, it breaks things in AWS.

- Fix checks on whether to write and use /etc/hosts

- Prevent installs fail when no md5 is available.

## 0.1.5

- Setup repo for releases with lein-pallet-release.

- Remove release code in prep for lein-pallet-release.

- Update to pallet 0.8.0-RC.9, vmfest 0.4.0-alpha.1 and locos 0.1.1.

- Release as EPL (was All Rights Reserved).

- Update to latest locos snapshot

## 0.1.4

- Update for pallet-0.8.0-beta.8

- Make use of /etc/hosts provider specific

## 0.1.3

- Modify hadoop-config-file to not log arguments

## 0.1.2

- Reduce potentially sensitive logs to trace

## 0.1.1

- Allow for running tests without a compute service

- Update to pallet 0.8.0-alpha.7

- Default s3 and s3n credentials
  Use the compute-service credentials for s3 credentials, if the compute
  service is ec2 based.

- Add missing use of debugf

- Reduce log level on unclassified properties

- Filter metrics properties correctly

- Use ips for namenode and jobtracker urls

- Update cloudera version

- Move url implementation for :cloudera

- Fix handling of args in hadoop-jar

- Add hadoop-role-ports
  A multi-method to report the required open ports for each role.


## 0.1.0

Initial release.
