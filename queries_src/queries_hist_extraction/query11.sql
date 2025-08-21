
SELECT
  codigoestacion,
  codigosensor,
  fechaobservacion,
  valorobservado,
  nombreestacion,
  departamento,
  municipio,
  zonahidrografica,
  latitud,
  longitud,
  descripcionsensor,
  unidadmedida
WHERE
  fechaobservacion BETWEEN "2025-07-01T00:00:00"::floating_timestamp AND "2025-08-09T23:45:00"::floating_timestamp
