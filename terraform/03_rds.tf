resource "aws_db_subnet_group" "rds" {
  name       = "${var.project}-rds-subnet-group"
  subnet_ids = [aws_subnet.private_az1.id, aws_subnet.private_az2.id]
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

  multi_az                = true   # AZ1 primary / AZ2 standby, matches your diagram
  publicly_accessible     = false
  backup_retention_period = 7
  skip_final_snapshot     = true
  deletion_protection     = false

  tags = { Name = "${var.project}-oltp" }
}
