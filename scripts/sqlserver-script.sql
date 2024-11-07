--1 criar tabase
CREATE DATABASE pocconnect;

--2 Usar database
USE pocconnect;

--3 Criar tabelas de teste
CREATE TABLE dbo.customer (
  id INT PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  source VARCHAR(20) NOT NULL
);

CREATE TABLE dbo.addresses (
  id INT PRIMARY KEY,
  street VARCHAR(250) NOT NULL,
  source VARCHAR(20) NOT NULL,
  customerId INT NOT NULL,
  FOREIGN KEY (customerId) REFERENCES customer(id)
);

CREATE TABLE dbo.composta (
    ID INT IDENTITY(1,1) NOT NULL,
    IDEMPRESA INT NOT NULL,
    SOURCE VARCHAR(50) NOT NULL,
    Nome VARCHAR(100),
    DataCriacao DATETIME DEFAULT GETDATE(),
    PRIMARY KEY (ID, IDEMPRESA)
);

--4 Habilitar CDC

EXEC sys.sp_cdc_enable_db
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customer', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'addresses', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'composta', @role_name = NULL, @supports_net_changes = 0;

--5 Inserir registros para test

INSERT INTO dbo.customer (id,name, source)
VALUES
  (1,'João Silva', 'SQLSERVER'),
  (2,'Maria Souza', 'SQLSERVER'),
  (3,'Pedro Oliveira', 'SQLSERVER'),
  (4,'Ana Costa', 'SQLSERVER'),
  (5,'Carlos Pereira', 'SQLSERVER'),
  (6,'Bruna Santos', 'SQLSERVER');

INSERT INTO dbo.address  (id, street, source, customer_id)
VALUES 
(1, 'Rua do Sol 1', 'SQLSERVER', 1),
(2, 'Rua do Sol 2', 'SQLSERVER', 1),
(3, 'Rua do Sol 3', 'SQLSERVER', 2),
(4, 'Rua do Sol 4', 'SQLSERVER', 2),
(5, 'Rua do Sol 5', 'SQLSERVER', 3),
(6, 'Rua do Sol 6', 'SQLSERVER', 3),
(7, 'Rua do Sol 7', 'SQLSERVER', 4),
(8, 'Rua do Sol 8', 'SQLSERVER', 4),
(9, 'Rua do Sol 9', 'SQLSERVER', 5),
(10, 'Rua do Sol 10', 'SQLSERVER', 5),
(11, 'Rua do Sol 11', 'SQLSERVER', 6)


--select para verificar registros
select * from dbo.customer order by id
SELECT * FROM dbo.address a INNER JOIN dbo.customer c ON c.Id = a.customer_id 


-- 6 inserir novo registro
insert  INTO dbo.customer (id, name, source) values (200, 'Irene De Pieri', 'SQLSERVER')


--7 update registro
UPDATE dbo.customer SET name = 'Irene De Pieri Diverth' where id =200


