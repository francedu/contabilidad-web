# Envío de comprobantes por correo

Cambios aplicados:

- En la vista **Pagos** se agrega botón **Enviar correo** junto al comprobante.
- El correo se envía al email guardado en la ficha del alumno.
- Se adjunta el comprobante PDF del pago.
- Si el alumno no tiene correo, se muestra aviso **Sin correo**.
- Solo usuarios con permisos de escritura (`admin`, `presidente`, `tesorero`, `secretario`) pueden enviar comprobantes.
- El envío queda registrado en auditoría como `enviar_correo / comprobante_pago`.

Variables SMTP necesarias en Render:

```text
SMTP_HOST=smtp.tudominio.com
SMTP_PORT=587
SMTP_USER=usuario@tudominio.com
SMTP_PASSWORD=clave
SMTP_FROM=ContaCurso <usuario@tudominio.com>
```

Notas:

- El botón usa el mismo PDF que se descarga desde **Comprobante**.
- No cambia la estructura de la base de datos.
