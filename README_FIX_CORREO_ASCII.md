# Fix correo SMTP ASCII / NBSP

Se corrige el error:

`'ascii' codec can't encode character '\xa0'`

Causa probable: valores copiados con espacios invisibles/no separables (NBSP) en correo, asunto, remitente o variables SMTP.

Cambios:
- Limpieza de `SMTP_HOST`, `SMTP_USER`, `SMTP_FROM`, destinatario, asunto y cuerpo.
- Eliminación de caracteres invisibles `\xa0`, `\u202f`, `\u200b`, `\ufeff`.
- Envío del cuerpo con `charset='utf-8'`.
- Validación de destinatario vacío.

Recomendación adicional en Render:
- Reescribir manualmente `SMTP_FROM`, `SMTP_USER` y correos de prueba, sin copiar/pegar desde Word/WhatsApp.
