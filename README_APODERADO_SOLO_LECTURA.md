# Rol apoderado solo lectura

Cambios aplicados:

- Se agrega `apoderado` como rol seleccionable en **Rol en ese curso**.
- El permiso se asigna por combinación `usuario + colegio + curso`, igual que presidente/tesorero/secretario.
- El apoderado puede ver la información del curso/colegio asignado, pero no puede crear, editar, eliminar ni enviar correos desde el sistema.
- Se actualiza la migración PostgreSQL para permitir `apoderado` en `usuario_roles_curso.rol_curso`.
- En SQLite se reconstruye la tabla `usuario_roles_curso` para permitir el nuevo valor del CHECK.

Uso recomendado:

1. Crear usuario.
2. Rol principal: `apoderado` o `solo_lectura`.
3. En permisos por colegio y curso, seleccionar:
   - Colegio
   - Curso
   - Rol en ese curso: `apoderado`

El usuario verá el curso asignado en modo solo lectura.
