# ContaCurso - Mejoras 4

Incluye:

- Módulo de cierres mensuales por colegio + curso + mes.
- Resumen congelado de ingresos, gastos, saldo, alumnos activos, pagos y deuda.
- Reporte PDF por cierre mensual.
- Eliminación de cierres solo para admin global.
- Auditoría al crear/eliminar cierres.

Uso recomendado:

1. Seleccionar colegio, curso y mes.
2. Crear cierre al terminar la revisión mensual.
3. Descargar PDF para acta/rendición.

La migración crea la tabla `cierres_mensuales` sin borrar datos existentes.
