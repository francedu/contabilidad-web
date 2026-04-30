# ContaCurso · Mejoras 5

Incluye:

- Notificaciones de deuda listas para copiar.
- Configuración de cuotas mensuales por colegio + curso + mes.
- Opción para aplicar la cuota mensual a todos los alumnos activos del curso.
- Bloqueo de edición/eliminación de pagos y movimientos cuando existe cierre mensual.
- Validaciones de monto positivo.

Notas:

- El rol `admin` puede modificar registros incluso en meses cerrados.
- Presidente y tesorero pueden configurar cuotas; secretario no puede configurar cuotas.
- La tabla nueva `cuotas_mensuales` se crea automáticamente al iniciar.
