# Fix Pagos / Aportes de actividad

Corrección aplicada:

- En la ficha del alumno, el historial ya no clasifica cuotas mensuales como aportes de actividad.
- Los aportes de actividad ahora se muestran solo cuando:
  - `movimientos.origen = 'actividad_alumno'`
  - `movimientos.actividad_id IS NOT NULL`
  - existe una actividad real asociada.
- El resumen de actividades ya no suma cuotas mensuales ni movimientos sin actividad como “Sin actividad”.
- Al registrar un aporte de actividad, el sistema exige que la actividad pertenezca al mismo colegio y curso del alumno.

No se eliminan datos existentes. Solo se corrige la visualización y se bloquea la creación de nuevos aportes inconsistentes.
