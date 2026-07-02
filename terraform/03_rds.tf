resource "aws_db_subnet_group" "rds" {
  name       = "${var.project}-rds-subnet-group"
  subnet_ids = [aws_subnet.private_az1.id, aws_subnet.private_az2.id]
}

# Custom parameter group to turn on logical replication for Airbyte CDC.
# rds.logical_replication is a "static" parameter in RDS -> it only takes
# effect after the instance reboots (see apply_immediately/reboot note below).
resource "aws_db_parameter_group" "olist_oltp_cdc" {
  name   = "${var.project}-oltp-cdc-pg"
  family = "postgres16"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  # Each CDC replication slot/publication needs a wal sender; default (10)
  # is usually fine but we make it explicit and give some headroom for
  # multiple Airbyte connections / re-syncs.
  parameter {
    name         = "max_replication_slots"
    value        = "20"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_wal_senders"
    value        = "20"
    apply_method = "pending-reboot"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_instance" "olist_oltp" {
  identifier             = "${var.project}-oltp"
  engine                 = "postgres"
  engine_version         = "16.4"
  instance_class         = "db.t3.medium"
  allocated_storage      = 50
  storage_type           = "gp3"
  db_name                = "olist"
  username               = var.db_username
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  parameter_group_name   = aws_db_parameter_group.olist_oltp_cdc.name

  multi_az                = true   # AZ1 primary / AZ2 standby, matches your diagram
  publicly_accessible     = false
  backup_retention_period = 7
  skip_final_snapshot     = true
  deletion_protection     = false

  # rds.logical_replication is static, so the FIRST apply that attaches this
  # parameter group still needs a reboot before CDC will actually work.
  # apply_immediately just makes sure other pending changes don't queue up
  # silently behind it - you still need to reboot once (see note in
  # 07_airbyte_ec2.tf) after the very first apply.
  apply_immediately = true

  tags = { Name = "${var.project}-oltp" }
}