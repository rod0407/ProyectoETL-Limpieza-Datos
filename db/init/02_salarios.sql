-- Tabla destino del pipeline_salarios.hpl (transform "Table output").
-- Los nombres de columna deben coincidir con los campos renombrados en "Select values".

USE etl_db;

CREATE TABLE IF NOT EXISTS salarios_limpios (
  anio              INT,
  nivel_experiencia VARCHAR(20),
  tipo_empleo       VARCHAR(20),
  puesto            VARCHAR(100),
  salario           BIGINT,
  moneda            CHAR(3),
  salario_usd       INT,
  pais_residencia   VARCHAR(60),
  modalidad         VARCHAR(12),
  pais_empresa      VARCHAR(60),
  tamano_empresa    VARCHAR(10)
) DEFAULT CHARSET = utf8mb4;
