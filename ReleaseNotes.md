# Release Notes

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
