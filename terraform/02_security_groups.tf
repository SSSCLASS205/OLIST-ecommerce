resource "aws_security_group" "rds_sg" {
  name_prefix = "${var.project}-rds-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.airbyte_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.project}-rds-sg" }
}

resource "aws_security_group" "airbyte_sg" {
  name_prefix = "${var.project}-airbyte-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Airbyte webapp from MWAA only"
    from_port   = 8000
    to_port     = 8001
    protocol    = "tcp"
    cidr_blocks = ["10.0.1.0/24"]
  }

  ingress {
    description = "Airbyte internal API from MWAA only (used by airbyte_sync_pipeline DAG)"
    from_port   = 8006
    to_port     = 8006
    protocol    = "tcp"
    cidr_blocks = ["10.0.1.0/24"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.project}-airbyte-sg" }
}

resource "aws_security_group" "mwaa_sg" {
  name_prefix = "${var.project}-mwaa-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Self-referencing for MWAA ENIs"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.project}-mwaa-sg" }
}