# Fix ingresos totales del alumno

La ficha del alumno ahora calcula la tarjeta "Ingresos totales del alumno" sumando:

- pagos_alumnos.monto para cuotas mensuales del alumno
- movimientos.monto de ingresos con origen actividad_alumno y actividad_id real

Esto corrige el caso donde solo aparecían los aportes de actividad ($2.000) y no las cuotas ($16.000).
