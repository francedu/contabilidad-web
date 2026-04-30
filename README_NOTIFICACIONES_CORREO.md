# Notificaciones por correo

Esta versión agrega correo del apoderado en la ficha del alumno y botones en Notificaciones:

- **Enviar correo**: envía desde el servidor usando SMTP.
- **Abrir en correo**: abre el cliente de correo del computador/celular con el mensaje prellenado.

## Configuración SMTP opcional

Para que el botón **Enviar correo** funcione, define estas variables de entorno:

```bash
export SMTP_HOST="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USER="tu_correo@gmail.com"
export SMTP_PASSWORD="clave_o_app_password"
export SMTP_FROM="tu_correo@gmail.com"
```

Si no configuras SMTP, el sistema mostrará un aviso y puedes usar **Abrir en correo**.
