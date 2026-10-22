-- Tabla destino del pipeline_examenes.hpl (transform "Table output").
-- MySQL ejecuta los scripts de esta carpeta al crear el contenedor por primera vez.
-- Los nombres de columna deben coincidir con los del pipeline (incluyen espacios).

USE etl_db;

CREATE TABLE IF NOT EXISTS examenes_limpios (
  `Nombre`            VARCHAR(100),
  `Direccion`         VARCHAR(255),
  `Mascota1`          VARCHAR(50),
  `Mascota2`          VARCHAR(50),
  `Amigo 1`           VARCHAR(50),
  `Amigo 2`           VARCHAR(50),
  `CURP`              CHAR(18),
  `Pasatiempo`        VARCHAR(100),
  `Carrera`           VARCHAR(100),
  `Facultad`          VARCHAR(100),
  `Cantante favorito` VARCHAR(100),
  `Edad`              INT,
  `Banda Favorita`    VARCHAR(100)
) DEFAULT CHARSET = utf8mb4;
