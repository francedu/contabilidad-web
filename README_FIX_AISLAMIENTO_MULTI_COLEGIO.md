# Fix aislamiento multi-colegio

Cambios aplicados:

- Los usuarios no-admin ya no caen por defecto al colegio `1`.
- Al crear/editar usuarios, `usuarios.colegio_id` y `usuarios.curso` se sincronizan con el primer permiso real de `usuario_roles_curso`.
- La barra superior muestra el colegio permitido del usuario, no el colegio por defecto.
- Los filtros de datos para usuarios normales usan únicamente los permisos `usuario + colegio + curso`.
- Si un usuario no tiene permisos ni colegio asignado, no ve información por seguridad.
- Se agrega reparación automática para usuarios antiguos: si tienen permisos por curso, se corrige su `colegio_id` al iniciar la app.

Recomendación después de desplegar:

1. Entrar como `admin`.
2. Abrir Usuarios.
3. Editar el usuario afectado y guardar nuevamente sus permisos.
4. Iniciar sesión con ese usuario y validar que arriba aparezca su colegio correcto.
