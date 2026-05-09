# Roles multi colegio

Esta versión usa el rol `admin` como administrador global de toda la plataforma.

## Roles

- `admin`: administra toda la plataforma, colegios, usuarios, respaldos y todos los datos.
- `presidente`: rol por combinación colegio + curso.
- `tesorero`: rol por combinación colegio + curso.
- `secretario`: rol por combinación colegio + curso.
- `solo_lectura`: acceso limitado según permisos asignados.

## Permisos por curso

Los permisos se guardan por combinación:

`usuario + colegio + curso + rol_curso`

Esto permite que existan cursos con el mismo nombre en colegios distintos, por ejemplo `1° Básico` en Colegio A y `1° Básico` en Colegio B.

## Migración

La migración conserva los datos actuales. Si existe un usuario con rol `admin_global`, lo convierte automáticamente a `admin` para mantener el acceso con el usuario `admin`.

Antes de actualizar en producción, respalda la base de datos.
