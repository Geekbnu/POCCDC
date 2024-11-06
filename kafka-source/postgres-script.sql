create schema poc;

--1 Criar tabela de teste
CREATE TABLE poc.customer (
  id INT NOT NULL,
  name VARCHAR(255) NOT NULL,
  source VARCHAR(255) NOT NULL,
  source_pk VARCHAR(255)  NULL,
  PRIMARY KEY (id)
);


CREATE TABLE poc.addresses (
  id SERIAL PRIMARY KEY,
  street VARCHAR(250) NOT NULL,
  source VARCHAR(20) NOT NULL,
  customerid INT NOT NULL,
  FOREIGN KEY (customerid) REFERENCES poc.customer(id)
);


CREATE TABLE poc.composta (
    ID INT not null,
    IDEMPRESA INT NOT NULL,
    SOURCE VARCHAR(50) NOT NULL,
    Nome VARCHAR(100),
    DataCriacao TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ID, IDEMPRESA)
);


-- 2 Inserir registro para teste
INSERT INTO poc.customer (id, name, source)
VALUES
  (1001, 'João Silva', 'POSTGRES'),
  (1002, 'Maria Gomes', 'POSTGRES'),
  (1003, 'Pedro Oliveira', 'POSTGRES'),
  (1004, 'Ana Souza', 'POSTGRES'),
  (1005, 'Carlos Pereira', 'POSTGRES'),
  (1006, 'Luisa Santos', 'POSTGRES'),
  (1007, 'Bruno Mendes', 'POSTGRES'),
  (1008, 'Amanda Costa', 'POSTGRES'),
  (1009, 'Ricardo Ferreira', 'POSTGRES'),
  (1010, 'Daniela Almeida', 'POSTGRES');
  
--visualizar registros
select * from poc.customer order by id;



--3 inserir registro para teste
insert  INTO poc.customer (id, name, source) values (10011, 'Luiz Antonio', 'POSTGRES')


--4 update registro para teste
UPDATE poc.customer
SET name = 'Luiz Antonio Kiosh'
WHERE id = 10011;



