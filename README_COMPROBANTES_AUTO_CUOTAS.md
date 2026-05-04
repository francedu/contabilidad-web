# Envío automático de comprobantes de cuota

Cambio aplicado:

- Al registrar un pago con tipo **cuota mensual**, el sistema genera el comprobante PDF y lo envía automáticamente al correo guardado en la ficha del alumno.
- Los aportes de actividades **no** se envían automáticamente.
- Si el alumno no tiene correo, el pago se registra igual y se muestra una advertencia.
- Si falla SMTP, el pago se registra igual y se muestra una advertencia; no se revierte el pago.
- El botón manual **Enviar comprobante** sigue disponible para reenvíos.

Requisitos en Render:

- SMTP_HOST
- SMTP_PORT
- SMTP_USER
- SMTP_PASSWORD
- SMTP_FROM

Para Gmail, SMTP_PASSWORD debe ser una contraseña de aplicación, sin espacios.
