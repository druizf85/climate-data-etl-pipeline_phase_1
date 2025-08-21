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
  fechaobservacion BETWEEN "2020-01-01T00:00:00"::floating_timestamp AND "2020-12-31T23:45:00"::floating_timestamp