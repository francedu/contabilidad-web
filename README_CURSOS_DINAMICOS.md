# Cursos dinámicos por colegio

Cambios aplicados:

- Nueva tabla `cursos` con `colegio_id`, `nombre`, `nivel`, `orden` y `activo`.
- Nuevo menú **Cursos** para el usuario `admin`.
- Cada colegio puede tener sus propios cursos, aunque compartan el mismo nombre.
- Se agregan cursos base desde Prekínder hasta 4° Medio para cada colegio.
- Se conservan cursos antiguos detectados en alumnos, movimientos, actividades y permisos.
- Los selectores de alumno y permisos de usuario ahora cargan cursos según el colegio seleccionado.
- Renombrar un curso desde el panel actualiza referencias existentes de ese mismo colegio.

Compatibilidad:

- No borra alumnos, pagos, movimientos ni permisos actuales.
- El campo `curso` sigue existiendo como texto para no romper reportes ni consultas actuales.
- La nueva tabla `cursos` ordena y controla las opciones disponibles por colegio.

Uso recomendado:

1. Entrar como `admin`.
2. Ir a **Cursos**.
3. Filtrar por colegio.
4. Activar, desactivar, crear o editar cursos.
5. Al crear alumnos o permisos de usuario, seleccionar colegio y luego curso.

Nota para producción:

Antes de subir a Render, hacer backup de PostgreSQL. La migración es aditiva: crea la tabla `cursos` y la llena con datos base/históricos.
