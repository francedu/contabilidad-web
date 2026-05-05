# Fix cursos/colegios/alumnos 500

Correcciones aplicadas:

- Se corrigió `templates/alumnos_form.html`: tenía dos bloques `{% block scripts %}`, lo que causaba `jinja2.exceptions.TemplateAssertionError: block 'scripts' defined twice` al abrir `/alumnos/nuevo`.
- Se corrigió `log_audit()` para aceptar `colegio_id` y `curso` opcionales. Algunas rutas de colegios/cursos enviaban esos parámetros y provocaban error interno después de guardar, por eso los datos se creaban pero la pantalla mostraba error.
- Se probó localmente:
  - `/alumnos/nuevo` abre correctamente.
  - Crear colegio responde 302 correcto.
  - Crear curso responde 302 correcto.
  - Crear alumno responde 302 correcto.

Notas:

- No modifica ni borra datos existentes.
- El deploy puede hacerse sobre PostgreSQL actual.
- Mantén backup antes de subir a producción.
