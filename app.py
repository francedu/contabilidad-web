from __future__ import annotations

import os
import re
import sqlite3
import subprocess
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import csv
import shutil
from io import BytesIO, StringIO

from flask import Flask, flash, g, redirect, render_template, request, send_file, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None
    RealDictCursor = None

from openpyxl import Workbook
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Spacer, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'instance' / 'contabilidad_curso.db'
INSTANCE_DIR = BASE_DIR / 'instance'
BACKUP_DIR = BASE_DIR / 'backups'
INSTANCE_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

SCHOOL_NAME = 'Escuela Las Mercedes'
SCHOOL_LOCATION = 'María Pinto'
ALLOWED_ROLES = ('admin', 'tesorero', 'solo_lectura')


class User(UserMixin):
    def __init__(self, row: Any):
        self.id = str(row['id'])
        self.username = row['username']
        self.password_hash = row['password_hash']
        self.role = row['role']
        self.nombre = row['nombre']
        self.activo = bool(row['activo'])

    def can_edit(self) -> bool:
        return self.role in ('admin', 'tesorero')

    def can_delete(self) -> bool:
        return self.role in ('admin', 'tesorero')

    def is_admin(self) -> bool:
        return self.role == 'admin'


class DBAdapter:
    def __init__(self, url: str):
        self.url = url
        self.kind = 'postgres' if url.startswith('postgresql://') or url.startswith('postgres://') else 'sqlite'
        self.conn = self._connect()

    def _connect(self):
        if self.kind == 'sqlite':
            conn = sqlite3.connect(self.url)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA foreign_keys = ON')
            return conn
        if psycopg2 is None:
            raise RuntimeError('psycopg2-binary no está instalado. Ejecuta: python3 -m pip install psycopg2-binary')
        conn = psycopg2.connect(self.url, cursor_factory=RealDictCursor)
        conn.autocommit = False
        return conn

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self):
        return self.conn.cursor()

    def _convert_sql(self, sql: str) -> str:
        if self.kind == 'sqlite':
            return sql
        return re.sub(r'\?', '%s', sql)

    def execute(self, sql: str, params: tuple | list | None = None):
        cur = self.conn.cursor()
        cur.execute(self._convert_sql(sql), params or [])
        return cur

    def executescript(self, script: str):
        if self.kind == 'sqlite':
            return self.conn.executescript(script)
        statements = [s.strip() for s in script.split(';') if s.strip()]
        cur = self.conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        return cur

    def fetchone(self, sql: str, params: tuple | list | None = None):
        cur = self.execute(sql, params)
        return cur.fetchone()

    def fetchall(self, sql: str, params: tuple | list | None = None):
        cur = self.execute(sql, params)
        return cur.fetchall()



def create_app() -> Flask:
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'cambia-esta-clave')
    app.config['DATABASE'] = os.environ.get('DATABASE_URL', str(DB_PATH))
    app.config['PREFERRED_URL_SCHEME'] = 'https'

    login_manager = LoginManager()
    login_manager.login_view = 'login'
    login_manager.login_message = 'Inicia sesión para continuar.'
    login_manager.login_message_category = 'warning'
    login_manager.init_app(app)

    @app.context_processor
    def inject_globals() -> dict[str, Any]:
        return {
            'SCHOOL_NAME': SCHOOL_NAME,
            'SCHOOL_LOCATION': SCHOOL_LOCATION,
            'now': datetime.now(),
            'formato_monto': formato_monto,
            'estado_cuota': estado_cuota,
            'current_user': current_user,
            'backup_dir': BACKUP_DIR,
            'db_engine': 'PostgreSQL' if is_postgres_url(app.config['DATABASE']) else 'SQLite',
        }

    def get_db() -> DBAdapter:
        if 'db' not in g:
            g.db = DBAdapter(app.config['DATABASE'])
        return g.db

    @login_manager.user_loader
    def load_user(user_id: str):
        db = get_db()
        row = db.fetchone('SELECT * FROM usuarios WHERE id = ? AND activo = 1', (int(user_id),))
        return User(row) if row else None

    @app.teardown_appcontext
    def close_db(_exc: Exception | None) -> None:
        db = g.pop('db', None)
        if db is not None:
            db.close()

    app.get_db = get_db  # type: ignore[attr-defined]

    with app.app_context():
        init_db(get_db())
        seed_default_admin(get_db())

    def role_required(*roles: str):
        def decorator(fn):
            @wraps(fn)
            @login_required
            def wrapper(*args, **kwargs):
                if current_user.role not in roles:
                    flash('No tienes permisos para realizar esta acción.', 'danger')
                    return redirect(url_for('dashboard'))
                return fn(*args, **kwargs)
            return wrapper
        return decorator

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for('dashboard'))
        if request.method == 'POST':
            username = request.form.get('username', '').strip().lower()
            password = request.form.get('password', '')
            db = get_db()
            row = db.fetchone('SELECT * FROM usuarios WHERE lower(username) = ? AND activo = 1', (username,))
            if row and check_password_hash(row['password_hash'], password):
                login_user(User(row), remember=True)
                flash(f'Bienvenido, {row["nombre"]}.', 'success')
                next_url = request.args.get('next')
                return redirect(next_url or url_for('dashboard'))
            flash('Usuario o contraseña incorrectos.', 'danger')
        return render_template('login.html')

    @app.get('/logout')
    @login_required
    def logout():
        logout_user()
        flash('Sesión cerrada.', 'success')
        return redirect(url_for('login'))

    @app.get('/healthz')
    def healthz():
        try:
            db = get_db()
            row = db.fetchone('SELECT 1 AS ok')
            ok = row['ok'] if row else 0
            return {'status': 'ok', 'db': int(ok)}, 200
        except Exception as exc:
            app.logger.exception('Health check failed')
            return {'status': 'error', 'detail': str(exc)}, 500

    @app.route('/')
    def index():
        return redirect(url_for('dashboard' if current_user.is_authenticated else 'login'))

    @app.route('/dashboard')
    @login_required
    def dashboard():
        db = get_db()
        resumen = db.fetchone(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tipo='ingreso' THEN monto ELSE 0 END),0) ingresos,
                COALESCE(SUM(CASE WHEN tipo='gasto' THEN monto ELSE 0 END),0) gastos,
                COUNT(*) cantidad
            FROM movimientos
            """
        )
        reporte = db.fetchall(
            """
            SELECT substr(fecha, 1, 7) AS mes,
                   COALESCE(SUM(CASE WHEN tipo = 'ingreso' THEN monto ELSE 0 END), 0) AS ingresos,
                   COALESCE(SUM(CASE WHEN tipo = 'gasto' THEN monto ELSE 0 END), 0) AS gastos
            FROM movimientos
            GROUP BY substr(fecha, 1, 7)
            ORDER BY mes ASC
            """
        )
        mes = request.args.get('mes') or datetime.today().strftime('%Y-%m')
        alertas = obtener_alertas_morosidad(db, mes)
        ultimos = db.fetchall(
            """
            SELECT m.id, m.fecha, m.tipo, m.concepto, m.monto, COALESCE(a.nombre, '-') AS actividad
            FROM movimientos m
            LEFT JOIN actividades a ON a.id = m.actividad_id
            ORDER BY m.fecha DESC, m.id DESC
            LIMIT 8
            """
        )
        backups = listar_backups()[:5]

        resumen_mes = db.fetchone(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tipo='ingreso' THEN monto ELSE 0 END),0) ingresos_mes,
                COALESCE(SUM(CASE WHEN tipo='gasto' THEN monto ELSE 0 END),0) gastos_mes,
                COUNT(*) movimientos_mes
            FROM movimientos
            WHERE substr(fecha, 1, 7) = ?
            """
            ,(mes,)
        )
        alumnos_activos = db.fetchone('SELECT COUNT(*) AS total FROM alumnos WHERE activo = 1')
        cuotas = resumen_cuotas_por_alumno(db, mes)
        total_esperado = sum(float(f['cuota_mensual']) for f in cuotas if f['activo'])
        total_pagado = sum(float(f['pagado']) for f in cuotas if f['activo'])
        deuda_total = sum(max(float(f['cuota_mensual']) - float(f['pagado']), 0) for f in cuotas if f['activo'])
        alumnos_pagados = 0
        alumnos_parciales = 0
        alumnos_deuda = 0
        for fila in cuotas:
            if not fila['activo']:
                continue
            estado, _icono = estado_cuota(fila['cuota_mensual'], fila['pagado'])
            if estado == 'Pagado':
                alumnos_pagados += 1
            elif estado == 'Parcial':
                alumnos_parciales += 1
            else:
                alumnos_deuda += 1

        ingresos_mes = float(resumen_mes['ingresos_mes'] or 0)
        gastos_mes = float(resumen_mes['gastos_mes'] or 0)
        balance_total = float(resumen['ingresos'] or 0) - float(resumen['gastos'] or 0)
        balance_mes = ingresos_mes - gastos_mes
        cumplimiento = round((total_pagado / total_esperado) * 100, 1) if total_esperado else 100.0
        ultimo_mes = dict(reporte[-1]) if reporte else {'mes': mes, 'ingresos': 0, 'gastos': 0}
        quick_actions = [
            {'label': 'Nuevo pago', 'href': url_for('pagos_new'), 'icon': '💳', 'hint': 'Registrar cuota o aporte'},
            {'label': 'Nuevo ingreso', 'href': url_for('movimientos_new') + '?tipo=ingreso', 'icon': '➕', 'hint': 'Agregar ingreso manual'},
            {'label': 'Nuevo gasto', 'href': url_for('movimientos_new') + '?tipo=gasto', 'icon': '🧾', 'hint': 'Registrar egreso'},
            {'label': 'Ver cuotas', 'href': url_for('cuotas_view', mes=mes), 'icon': '📌', 'hint': 'Revisar estado mensual'},
        ]

        dashboard_stats = {
            'balance_total': balance_total,
            'balance_mes': balance_mes,
            'deuda_total': deuda_total,
            'alumnos_activos': int(alumnos_activos['total'] or 0),
            'alumnos_pagados': alumnos_pagados,
            'alumnos_parciales': alumnos_parciales,
            'alumnos_deuda': alumnos_deuda,
            'ingresos_mes': ingresos_mes,
            'gastos_mes': gastos_mes,
            'movimientos_mes': int(resumen_mes['movimientos_mes'] or 0),
            'cumplimiento': cumplimiento,
            'ultimo_mes': ultimo_mes,
            'total_esperado': total_esperado,
            'total_pagado': total_pagado,
        }

        return render_template(
            'dashboard.html',
            resumen=resumen,
            reporte=reporte,
            mes=mes,
            alertas=alertas,
            ultimos=ultimos,
            backups=backups,
            dashboard_stats=dashboard_stats,
            quick_actions=quick_actions,
        )

    @app.route('/movimientos/export/<fmt>')
    @login_required
    def movimientos_export(fmt: str):
        db = get_db()
        tipo = request.args.get('tipo', 'Todos')
        mes = request.args.get('mes', '')
        q = request.args.get('q', '').strip()
        movimientos = obtener_movimientos_filtrados(db, tipo=tipo, mes=mes, q=q)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre = f'movimientos_{ts}'
        if fmt == 'csv':
            sio = StringIO()
            writer = csv.writer(sio)
            writer.writerow(['ID', 'Fecha', 'Tipo', 'Concepto', 'Actividad', 'Alumno', 'Origen', 'Monto', 'Observación'])
            for row in movimientos:
                writer.writerow([row['id'], row['fecha'], row['tipo'], row['concepto'], row['actividad'], row.get('alumno', '-'), row['origen'], row['monto'], row['observacion']])
            data = BytesIO(sio.getvalue().encode('utf-8-sig'))
            return send_file(data, mimetype='text/csv', as_attachment=True, download_name=f'{nombre}.csv')
        if fmt == 'xlsx':
            wb = Workbook()
            ws = wb.active
            ws.title = 'Movimientos'
            ws.append(['ID', 'Fecha', 'Tipo', 'Concepto', 'Actividad', 'Alumno', 'Origen', 'Monto', 'Observación'])
            for row in movimientos:
                ws.append([row['id'], row['fecha'], row['tipo'], row['concepto'], row['actividad'], row.get('alumno', '-'), row['origen'], float(row['monto']), row['observacion']])
            for cell in ws[1]:
                cell.font = cell.font.copy(bold=True)
            for column in ['A','B','C','D','E','F','G','H']:
                ws.column_dimensions[column].width = 18 if column != 'D' else 34
            data = BytesIO()
            wb.save(data)
            data.seek(0)
            return send_file(data, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=f'{nombre}.xlsx')
        if fmt == 'pdf':
            data = exportar_movimientos_pdf(movimientos, SCHOOL_NAME, SCHOOL_LOCATION, {'Tipo': tipo, 'Mes': mes or 'Todos', 'Búsqueda': q or 'Todos'})
            return send_file(data, mimetype='application/pdf', as_attachment=True, download_name=f'{nombre}.pdf')
        flash('Formato de exportación no soportado.', 'danger')
        return redirect(url_for('movimientos_list', tipo=tipo, mes=mes, q=q))

    @app.route('/backups')
    @role_required('admin')
    def backups_list():
        backups = listar_backups()
        return render_template('backups.html', backups=backups)

    @app.post('/backups/crear')
    @role_required('admin')
    def backups_create():
        try:
            ruta = crear_backup_db(app.config['DATABASE'])
            flash(f'Respaldo creado: {ruta.name}', 'success')
        except Exception as exc:
            flash(f'No se pudo crear el respaldo: {exc}', 'danger')
        return redirect(request.referrer or url_for('backups_list'))

    @app.get('/backups/<path:nombre>')
    @role_required('admin')
    def backups_download(nombre: str):
        ruta = BACKUP_DIR / nombre
        if not ruta.exists() or ruta.parent != BACKUP_DIR:
            flash('Respaldo no encontrado.', 'danger')
            return redirect(url_for('backups_list'))
        return send_file(ruta, as_attachment=True, download_name=ruta.name)

    @app.route('/usuarios')
    @role_required('admin')
    def usuarios_list():
        db = get_db()
        usuarios = db.fetchall('SELECT id, username, nombre, role, activo FROM usuarios ORDER BY nombre, username')
        return render_template('usuarios_list.html', usuarios=usuarios)

    @app.route('/usuarios/nuevo', methods=['GET', 'POST'])
    @role_required('admin')
    def usuarios_new():
        db = get_db()
        if request.method == 'POST':
            nombre = request.form.get('nombre', '').strip()
            username = request.form.get('username', '').strip().lower()
            password = request.form.get('password', '')
            role = request.form.get('role', 'solo_lectura')
            activo = 1 if request.form.get('activo') == 'on' else 0
            if not nombre or not username or not password:
                flash('Nombre, usuario y contraseña son obligatorios.', 'danger')
            elif role not in ALLOWED_ROLES:
                flash('Rol inválido.', 'danger')
            elif db.fetchone('SELECT 1 FROM usuarios WHERE lower(username)=?', (username,)):
                flash('Ese nombre de usuario ya existe.', 'danger')
            else:
                db.execute(
                    'INSERT INTO usuarios (username, password_hash, role, nombre, activo) VALUES (?, ?, ?, ?, ?)',
                    (username, generate_password_hash(password), role, nombre, activo)
                )
                db.commit()
                flash('Usuario creado.', 'success')
                return redirect(url_for('usuarios_list'))
        return render_template('usuarios_form.html', usuario=None, roles=ALLOWED_ROLES)

    @app.route('/usuarios/<int:user_id>/editar', methods=['GET', 'POST'])
    @role_required('admin')
    def usuarios_edit(user_id: int):
        db = get_db()
        usuario = db.fetchone('SELECT id, username, role, nombre, activo FROM usuarios WHERE id=?', (user_id,))
        if not usuario:
            flash('Usuario no encontrado.', 'danger')
            return redirect(url_for('usuarios_list'))
        if request.method == 'POST':
            nombre = request.form.get('nombre', '').strip()
            username = request.form.get('username', '').strip().lower()
            password = request.form.get('password', '')
            role = request.form.get('role', 'solo_lectura')
            activo = 1 if request.form.get('activo') == 'on' else 0
            if not nombre or not username:
                flash('Nombre y usuario son obligatorios.', 'danger')
            elif role not in ALLOWED_ROLES:
                flash('Rol inválido.', 'danger')
            elif db.fetchone('SELECT 1 FROM usuarios WHERE lower(username)=? AND id<>?', (username, user_id)):
                flash('Ese nombre de usuario ya existe.', 'danger')
            else:
                if password:
                    db.execute('UPDATE usuarios SET nombre=?, username=?, role=?, activo=?, password_hash=? WHERE id=?',
                               (nombre, username, role, activo, generate_password_hash(password), user_id))
                else:
                    db.execute('UPDATE usuarios SET nombre=?, username=?, role=?, activo=? WHERE id=?',
                               (nombre, username, role, activo, user_id))
                db.commit()
                flash('Usuario actualizado.', 'success')
                return redirect(url_for('usuarios_list'))
        return render_template('usuarios_form.html', usuario=usuario, roles=ALLOWED_ROLES)

    @app.post('/usuarios/<int:user_id>/eliminar')
    @role_required('admin')
    def usuarios_delete(user_id: int):
        if int(current_user.id) == user_id:
            flash('No puedes eliminar tu propio usuario.', 'danger')
            return redirect(url_for('usuarios_list'))
        db = get_db()
        db.execute('DELETE FROM usuarios WHERE id=?', (user_id,))
        db.commit()
        flash('Usuario eliminado.', 'success')
        return redirect(url_for('usuarios_list'))

    @app.route('/alumnos')
    @login_required
    def alumnos_list():
        db = get_db()
        q = request.args.get('q', '').strip()
        mes = request.args.get('mes') or datetime.today().strftime('%Y-%m')
        sql = """
            SELECT a.id, a.nombre, a.curso, a.cuota_mensual, a.activo,
                   COALESCE(SUM(CASE WHEN p.mes = ? THEN p.monto ELSE 0 END), 0) AS pagado_mes
            FROM alumnos a
            LEFT JOIN pagos_alumnos p ON p.alumno_id = a.id
            WHERE 1=1
        """
        params: list[Any] = [mes]
        if q:
            sql += " AND (LOWER(COALESCE(a.nombre, '')) LIKE ? OR LOWER(COALESCE(a.curso, '')) LIKE ?)"
            like = sql_like_ci(q)
            params.extend([like, like])
        sql += ' GROUP BY a.id, a.nombre, a.curso, a.cuota_mensual, a.activo ORDER BY a.nombre'
        alumnos = db.fetchall(sql, params)
        deuda_total = sum(max(float(a['cuota_mensual']) - float(a['pagado_mes']), 0) for a in alumnos if a['activo'])
        return render_template('alumnos_list.html', alumnos=alumnos, q=q, mes=mes, deuda_total=deuda_total)

    @app.route('/alumnos/nuevo', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def alumnos_new():
        db = get_db()
        if request.method == 'POST':
            nombre = request.form.get('nombre', '').strip()
            curso = request.form.get('curso', '').strip()
            cuota = parse_float(request.form.get('cuota_mensual', '0'))
            activo = 1 if request.form.get('activo') == 'on' else 0
            if not nombre:
                flash('El nombre es obligatorio.', 'danger')
            elif alumno_duplicado(db, nombre, curso):
                flash('Ya existe un alumno con ese nombre y curso.', 'danger')
            else:
                db.execute(
                    'INSERT INTO alumnos (nombre, curso, cuota_mensual, activo) VALUES (?, ?, ?, ?)',
                    (nombre, curso, cuota, activo),
                )
                db.commit()
                flash('Alumno creado correctamente.', 'success')
                return redirect(url_for('alumnos_list'))
        return render_template('alumnos_form.html', alumno=None)

    @app.route('/alumnos/<int:alumno_id>/editar', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def alumnos_edit(alumno_id: int):
        db = get_db()
        alumno = db.fetchone('SELECT * FROM alumnos WHERE id = ?', (alumno_id,))
        if not alumno:
            flash('Alumno no encontrado.', 'danger')
            return redirect(url_for('alumnos_list'))
        if request.method == 'POST':
            nombre = request.form.get('nombre', '').strip()
            curso = request.form.get('curso', '').strip()
            cuota = parse_float(request.form.get('cuota_mensual', '0'))
            activo = 1 if request.form.get('activo') == 'on' else 0
            if not nombre:
                flash('El nombre es obligatorio.', 'danger')
            elif alumno_duplicado(db, nombre, curso, exclude_id=alumno_id):
                flash('Ya existe otro alumno con ese nombre y curso.', 'danger')
            else:
                db.execute(
                    'UPDATE alumnos SET nombre = ?, curso = ?, cuota_mensual = ?, activo = ? WHERE id = ?',
                    (nombre, curso, cuota, activo, alumno_id),
                )
                db.commit()
                flash('Alumno actualizado.', 'success')
                return redirect(url_for('alumnos_list'))
        return render_template('alumnos_form.html', alumno=alumno)

    @app.post('/alumnos/<int:alumno_id>/eliminar')
    @role_required('admin', 'tesorero')
    def alumnos_delete(alumno_id: int):
        db = get_db()
        alumno = db.fetchone('SELECT nombre FROM alumnos WHERE id=?', (alumno_id,))
        if not alumno:
            flash('Alumno no encontrado.', 'danger')
            return redirect(url_for('alumnos_list'))
        db.execute('DELETE FROM pagos_alumnos WHERE alumno_id = ?', (alumno_id,))
        db.execute('DELETE FROM alumnos WHERE id = ?', (alumno_id,))
        db.commit()
        flash(f'Alumno eliminado: {alumno["nombre"]}.', 'success')
        return redirect(url_for('alumnos_list'))

    @app.route('/alumnos/<int:alumno_id>')
    @login_required
    def alumno_detail(alumno_id: int):
        db = get_db()
        alumno = db.fetchone('SELECT * FROM alumnos WHERE id = ?', (alumno_id,))
        if not alumno:
            flash('Alumno no encontrado.', 'danger')
            return redirect(url_for('alumnos_list'))
        mes_actual = request.args.get('mes') or datetime.today().strftime('%Y-%m')
        resumen_mes = db.fetchone(
            """
            SELECT COALESCE(SUM(CASE WHEN p.mes = ? THEN p.monto ELSE 0 END), 0) AS pagado_mes,
                   COUNT(CASE WHEN p.mes = ? THEN 1 END) AS pagos_mes
            FROM pagos_alumnos p
            WHERE p.alumno_id = ?
            """,
            (mes_actual, mes_actual, alumno_id),
        )
        resumen_aportes = db.fetchone(
            """
            SELECT COALESCE(SUM(CASE WHEN m.tipo = 'ingreso' THEN m.monto ELSE 0 END), 0) AS ingresos_actividad,
                   COALESCE(SUM(CASE WHEN m.tipo = 'gasto' THEN m.monto ELSE 0 END), 0) AS gastos_asociados,
                   COUNT(*) AS movimientos_asociados
            FROM movimientos m
            WHERE m.alumno_id = ?
            """,
            (alumno_id,),
        )
        historial_cuotas = db.fetchall(
            """
            SELECT p.id, p.fecha, p.mes, p.monto, p.observacion, 'Cuota mensual' AS tipo, NULL AS actividad
            FROM pagos_alumnos p
            WHERE p.alumno_id = ?
            ORDER BY p.fecha DESC, p.id DESC
            """,
            (alumno_id,),
        )
        historial_aportes = db.fetchall(
            """
            SELECT m.id, m.fecha, substr(m.fecha,1,7) AS mes, m.monto, m.observacion,
                   CASE WHEN m.tipo = 'gasto' THEN 'Egreso asociado' ELSE 'Aporte actividad' END AS tipo,
                   COALESCE(a.nombre, '-') AS actividad
            FROM movimientos m
            LEFT JOIN actividades a ON a.id = m.actividad_id
            WHERE m.alumno_id = ?
               OR (m.origen = 'actividad_alumno' AND m.alumno_id IS NULL AND LOWER(m.concepto) LIKE ?)
            ORDER BY m.fecha DESC, m.id DESC
            """,
            (alumno_id, sql_like_ci(f'Aporte actividad alumno: {alumno["nombre"]}')[:-1] + '%'),
        )
        historial = sorted([dict(x) for x in historial_cuotas] + [dict(x) for x in historial_aportes], key=lambda x: (x['fecha'], x['id']), reverse=True)
        actividad_resumen = db.fetchall(
            """
            SELECT COALESCE(a.id, 0) AS actividad_id, COALESCE(a.nombre, 'Sin actividad') AS actividad,
                   COALESCE(SUM(CASE WHEN m.tipo = 'ingreso' THEN m.monto ELSE 0 END), 0) AS ingresos,
                   COALESCE(SUM(CASE WHEN m.tipo = 'gasto' THEN m.monto ELSE 0 END), 0) AS egresos,
                   COUNT(*) AS movimientos
            FROM movimientos m
            LEFT JOIN actividades a ON a.id = m.actividad_id
            WHERE m.alumno_id = ?
            GROUP BY a.id, a.nombre
            ORDER BY actividad
            """,
            (alumno_id,),
        )
        deuda_mes = max(float(alumno['cuota_mensual']) - float(resumen_mes['pagado_mes'] or 0), 0) if alumno['activo'] else 0
        resumen = {
            'mes': mes_actual,
            'pagado_mes': float(resumen_mes['pagado_mes'] or 0),
            'pagos_mes': int(resumen_mes['pagos_mes'] or 0),
            'deuda_mes': deuda_mes,
            'ingresos_actividad': float(resumen_aportes['ingresos_actividad'] or 0),
            'gastos_asociados': float(resumen_aportes['gastos_asociados'] or 0),
            'movimientos_asociados': int(resumen_aportes['movimientos_asociados'] or 0),
        }
        return render_template('alumno_detail.html', alumno=alumno, historial=historial, resumen=resumen, actividad_resumen=actividad_resumen)

    @app.route('/pagos')
    @login_required
    def pagos_list():
        db = get_db()
        mes = request.args.get('mes', '').strip()
        sql = """
            SELECT p.id, p.alumno_id, a.nombre, a.curso, p.fecha, p.mes, p.monto, p.observacion, p.movimiento_id
            FROM pagos_alumnos p
            INNER JOIN alumnos a ON a.id = p.alumno_id
            WHERE 1=1
        """
        params: list[Any] = []
        if mes:
            sql += ' AND p.mes = ?'
            params.append(mes)
        sql += ' ORDER BY p.fecha DESC, a.nombre'
        pagos = db.fetchall(sql, params)
        return render_template('pagos_list.html', pagos=pagos, mes=mes)

    @app.route('/pagos/nuevo', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def pagos_new():
        db = get_db()
        alumnos = db.fetchall('SELECT id, nombre, curso, cuota_mensual FROM alumnos WHERE activo = 1 ORDER BY nombre')
        actividades = db.fetchall('SELECT id, nombre, fecha FROM actividades ORDER BY fecha DESC, nombre')
        if request.method == 'POST':
            alumno_id = int(request.form.get('alumno_id', '0') or 0)
            fecha = request.form.get('fecha', '').strip()
            mes = request.form.get('mes', '').strip()
            monto = parse_float(request.form.get('monto', '0'))
            observacion = request.form.get('observacion', '').strip()
            tipo_pago = request.form.get('tipo_pago', 'cuota_mensual')
            actividad_raw = request.form.get('actividad_id', '').strip()
            actividad_id = int(actividad_raw) if actividad_raw else None
            try:
                validar_fecha(fecha)
                datetime.strptime(mes + '-01', '%Y-%m-%d')
            except Exception:
                flash('Fecha o mes inválido.', 'danger')
                return render_template('pagos_form.html', alumnos=alumnos, actividades=actividades, pago=None)
            if tipo_pago == 'actividad_alumno' and not actividad_id:
                flash('Debes seleccionar una actividad para un aporte.', 'danger')
            elif tipo_pago == 'cuota_mensual' and pago_duplicado(db, alumno_id, mes):
                flash('Ese alumno ya tiene un pago registrado para ese mes.', 'danger')
            else:
                registrar_pago_alumno(db, alumno_id, fecha, mes, monto, observacion, actividad_id, tipo_pago)
                db.commit()
                flash('Pago registrado correctamente.', 'success')
                return redirect(url_for('pagos_list'))
        return render_template('pagos_form.html', alumnos=alumnos, actividades=actividades, pago=None)

    @app.route('/pagos/<int:pago_id>/editar', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def pagos_edit(pago_id: int):
        db = get_db()
        pago = db.fetchone('SELECT * FROM pagos_alumnos WHERE id=?', (pago_id,))
        if not pago:
            flash('Pago no encontrado.', 'danger')
            return redirect(url_for('pagos_list'))
        alumnos = db.fetchall('SELECT id, nombre, curso, cuota_mensual FROM alumnos WHERE activo = 1 OR id = ? ORDER BY nombre', (pago['alumno_id'],))
        actividades = db.fetchall('SELECT id, nombre, fecha FROM actividades ORDER BY fecha DESC, nombre')
        if request.method == 'POST':
            alumno_id = int(request.form.get('alumno_id', '0') or 0)
            fecha = request.form.get('fecha', '').strip()
            mes = request.form.get('mes', '').strip()
            monto = parse_float(request.form.get('monto', '0'))
            observacion = request.form.get('observacion', '').strip()
            try:
                validar_fecha(fecha)
                datetime.strptime(mes + '-01', '%Y-%m-%d')
            except Exception:
                flash('Fecha o mes inválido.', 'danger')
                return render_template('pagos_form.html', alumnos=alumnos, actividades=actividades, pago=pago)
            if db.fetchone('SELECT 1 FROM pagos_alumnos WHERE alumno_id=? AND mes=? AND id<>?', (alumno_id, mes, pago_id)):
                flash('Ese alumno ya tiene otro pago registrado para ese mes.', 'danger')
            else:
                db.execute('UPDATE pagos_alumnos SET alumno_id=?, fecha=?, mes=?, monto=?, observacion=? WHERE id=?',
                           (alumno_id, fecha, mes, monto, observacion, pago_id))
                db.execute('UPDATE movimientos SET fecha=?, concepto=?, monto=?, alumno_id=?, observacion=? WHERE id=?',
                           (fecha, f'Cuota mensual alumno: {obtener_nombre_alumno(db, alumno_id)} ({mes})', monto, alumno_id, observacion, pago['movimiento_id']))
                db.commit()
                flash('Pago actualizado.', 'success')
                return redirect(url_for('pagos_list'))
        return render_template('pagos_form.html', alumnos=alumnos, actividades=actividades, pago=pago)

    @app.post('/pagos/<int:pago_id>/eliminar')
    @role_required('admin', 'tesorero')
    def pagos_delete(pago_id: int):
        db = get_db()
        pago = db.fetchone('SELECT * FROM pagos_alumnos WHERE id=?', (pago_id,))
        if not pago:
            flash('Pago no encontrado.', 'danger')
            return redirect(url_for('pagos_list'))
        db.execute('DELETE FROM pagos_alumnos WHERE id=?', (pago_id,))
        if pago['movimiento_id']:
            db.execute('DELETE FROM movimientos WHERE id=?', (pago['movimiento_id'],))
        db.commit()
        flash('Pago eliminado.', 'success')
        return redirect(url_for('pagos_list'))

    @app.route('/movimientos')
    @login_required
    def movimientos_list():
        db = get_db()
        tipo = request.args.get('tipo', 'Todos')
        mes = request.args.get('mes', '')
        q = request.args.get('q', '').strip()
        fecha_desde = request.args.get('fecha_desde', '').strip()
        fecha_hasta = request.args.get('fecha_hasta', '').strip()
        actividad_id = request.args.get('actividad_id', '').strip()
        alumno_id = request.args.get('alumno_id', '').strip()
        movimientos = obtener_movimientos_filtrados(db, tipo=tipo, mes=mes, q=q, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, actividad_id=actividad_id, alumno_id=alumno_id)
        actividades = db.fetchall('SELECT id, nombre, fecha FROM actividades ORDER BY fecha DESC, nombre')
        alumnos = db.fetchall('SELECT id, nombre, curso FROM alumnos WHERE activo = 1 ORDER BY nombre')
        return render_template('movimientos_list.html', movimientos=movimientos, tipo=tipo, mes=mes, q=q, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, actividad_id=actividad_id, alumno_id=alumno_id, actividades=actividades, alumnos=alumnos)

    @app.route('/movimientos/nuevo', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def movimientos_new():
        db = get_db()
        actividades = db.fetchall('SELECT id, nombre, fecha FROM actividades ORDER BY fecha DESC, nombre')
        alumnos = db.fetchall('SELECT id, nombre, curso FROM alumnos WHERE activo = 1 ORDER BY nombre')
        if request.method == 'POST':
            fecha = request.form.get('fecha', '').strip()
            tipo = request.form.get('tipo', 'ingreso').strip()
            concepto = request.form.get('concepto', '').strip()
            monto = parse_float(request.form.get('monto', '0'))
            actividad_raw = request.form.get('actividad_id', '').strip()
            actividad_id = int(actividad_raw) if actividad_raw else None
            alumno_raw = request.form.get('alumno_id', '').strip()
            alumno_id = int(alumno_raw) if alumno_raw else None
            observacion = request.form.get('observacion', '').strip()
            try:
                validar_fecha(fecha)
            except Exception:
                flash('Fecha inválida.', 'danger')
                return render_template('movimientos_form.html', actividades=actividades, alumnos=alumnos, movimiento=None)
            db.execute(
                'INSERT INTO movimientos (fecha, tipo, concepto, monto, actividad_id, alumno_id, observacion, origen) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (fecha, tipo, concepto, monto, actividad_id, alumno_id, observacion, 'general'),
            )
            db.commit()
            flash('Movimiento creado.', 'success')
            next_url = request.form.get('next', '').strip()
            return redirect(next_url or url_for('movimientos_list'))
        return render_template('movimientos_form.html', actividades=actividades, alumnos=alumnos, movimiento=None, next_url=request.args.get('next', ''), selected_alumno_id=request.args.get('alumno_id', ''))

    @app.route('/movimientos/<int:movimiento_id>/editar', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def movimientos_edit(movimiento_id: int):
        db = get_db()
        movimiento = db.fetchone('SELECT * FROM movimientos WHERE id=?', (movimiento_id,))
        if not movimiento:
            flash('Movimiento no encontrado.', 'danger')
            return redirect(url_for('movimientos_list'))
        actividades = db.fetchall('SELECT id, nombre, fecha FROM actividades ORDER BY fecha DESC, nombre')
        alumnos = db.fetchall('SELECT id, nombre, curso FROM alumnos WHERE activo = 1 OR id = ? ORDER BY nombre', (movimiento['alumno_id'] or 0,))
        if request.method == 'POST':
            fecha = request.form.get('fecha', '').strip()
            tipo = request.form.get('tipo', 'ingreso').strip()
            concepto = request.form.get('concepto', '').strip()
            monto = parse_float(request.form.get('monto', '0'))
            actividad_raw = request.form.get('actividad_id', '').strip()
            actividad_id = int(actividad_raw) if actividad_raw else None
            alumno_raw = request.form.get('alumno_id', '').strip()
            alumno_id = int(alumno_raw) if alumno_raw else None
            observacion = request.form.get('observacion', '').strip()
            try:
                validar_fecha(fecha)
            except Exception:
                flash('Fecha inválida.', 'danger')
                return render_template('movimientos_form.html', actividades=actividades, alumnos=alumnos, movimiento=movimiento)
            db.execute('UPDATE movimientos SET fecha=?, tipo=?, concepto=?, monto=?, actividad_id=?, alumno_id=?, observacion=? WHERE id=?',
                       (fecha, tipo, concepto, monto, actividad_id, alumno_id, observacion, movimiento_id))
            db.commit()
            flash('Movimiento actualizado.', 'success')
            next_url = request.form.get('next', '').strip()
            return redirect(next_url or url_for('movimientos_list'))
        return render_template('movimientos_form.html', actividades=actividades, alumnos=alumnos, movimiento=movimiento, next_url=request.args.get('next', ''), selected_alumno_id=request.args.get('alumno_id', ''))

    @app.post('/movimientos/<int:movimiento_id>/eliminar')
    @role_required('admin', 'tesorero')
    def movimientos_delete(movimiento_id: int):
        db = get_db()
        movimiento = db.fetchone('SELECT concepto FROM movimientos WHERE id=?', (movimiento_id,))
        if not movimiento:
            flash('Movimiento no encontrado.', 'danger')
            return redirect(url_for('movimientos_list'))
        db.execute('DELETE FROM pagos_alumnos WHERE movimiento_id = ?', (movimiento_id,))
        db.execute('DELETE FROM movimientos WHERE id = ?', (movimiento_id,))
        db.commit()
        flash(f'Movimiento eliminado: {movimiento["concepto"]}.', 'success')
        next_url = request.form.get('next', '').strip()
        return redirect(next_url or url_for('movimientos_list'))

    @app.route('/actividades')
    @login_required
    def actividades_list():
        db = get_db()
        actividades = db.fetchall(
            """
            SELECT
                a.id,
                a.nombre,
                a.fecha,
                COALESCE(a.descripcion, '') AS descripcion,
                COALESCE(SUM(CASE WHEN m.tipo = 'ingreso' THEN m.monto ELSE 0 END), 0) AS ingresos,
                COALESCE(SUM(CASE WHEN m.tipo = 'gasto' THEN m.monto ELSE 0 END), 0) AS egresos,
                COALESCE(SUM(CASE WHEN m.tipo = 'ingreso' THEN 1 ELSE 0 END), 0) AS cantidad_ingresos,
                COALESCE(SUM(CASE WHEN m.tipo = 'gasto' THEN 1 ELSE 0 END), 0) AS cantidad_egresos
            FROM actividades a
            LEFT JOIN movimientos m ON m.actividad_id = a.id
            GROUP BY a.id, a.nombre, a.fecha, a.descripcion
            ORDER BY a.fecha DESC, a.nombre
            """
        )
        resumen_general = {
            'ingresos': sum(float(a['ingresos'] or 0) for a in actividades),
            'egresos': sum(float(a['egresos'] or 0) for a in actividades),
            'cantidad_actividades': len(actividades),
        }
        resumen_general['balance'] = resumen_general['ingresos'] - resumen_general['egresos']
        return render_template('actividades_list.html', actividades=actividades, resumen_general=resumen_general)

    @app.route('/reportes/actividades')
    @login_required
    def actividades_report():
        db = get_db()
        mes = request.args.get('mes') or datetime.today().strftime('%Y-%m')
        actividades = db.fetchall(
            """
            SELECT
                a.id, a.nombre, a.fecha, COALESCE(a.descripcion, '') AS descripcion,
                COALESCE(SUM(CASE WHEN m.tipo = 'ingreso' THEN m.monto ELSE 0 END), 0) AS ingresos,
                COALESCE(SUM(CASE WHEN m.tipo = 'gasto' THEN m.monto ELSE 0 END), 0) AS egresos,
                COUNT(m.id) AS movimientos
            FROM actividades a
            LEFT JOIN movimientos m ON m.actividad_id = a.id
            GROUP BY a.id, a.nombre, a.fecha, a.descripcion
            ORDER BY a.fecha DESC, a.nombre
            """
        )
        deudas = resumen_cuotas_por_alumno(db, mes)
        total_deuda = sum(max(float(f['cuota_mensual']) - float(f['pagado']), 0) for f in deudas if f['activo'])
        return render_template('actividades_report.html', actividades=actividades, mes=mes, deudas=deudas, total_deuda=total_deuda)

    @app.route('/actividades/<int:actividad_id>')
    @login_required
    def actividad_detail(actividad_id: int):
        db = get_db()
        actividad = db.fetchone(
            """
            SELECT id, nombre, fecha, COALESCE(descripcion, '') AS descripcion
            FROM actividades
            WHERE id = ?
            """
            , (actividad_id,)
        )
        if not actividad:
            flash('Actividad no encontrada.', 'danger')
            return redirect(url_for('actividades_list'))

        resumen = db.fetchone(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tipo = 'ingreso' THEN monto ELSE 0 END), 0) AS ingresos,
                COALESCE(SUM(CASE WHEN tipo = 'gasto' THEN monto ELSE 0 END), 0) AS gastos,
                COALESCE(SUM(CASE WHEN tipo = 'ingreso' THEN 1 ELSE 0 END), 0) AS cantidad_ingresos,
                COALESCE(SUM(CASE WHEN tipo = 'gasto' THEN 1 ELSE 0 END), 0) AS cantidad_gastos
            FROM movimientos
            WHERE actividad_id = ?
            """
            , (actividad_id,)
        )
        ingresos = db.fetchall(
            """
            SELECT m.id, m.fecha, m.concepto, m.monto, COALESCE(m.observacion, '') AS observacion,
                   COALESCE(m.origen, 'general') AS origen, COALESCE(al.nombre, '-') AS alumno
            FROM movimientos m
            LEFT JOIN alumnos al ON al.id = m.alumno_id
            WHERE m.actividad_id = ? AND m.tipo = 'ingreso'
            ORDER BY m.fecha DESC, m.id DESC
            """
            , (actividad_id,)
        )
        gastos = db.fetchall(
            """
            SELECT m.id, m.fecha, m.concepto, m.monto, COALESCE(m.observacion, '') AS observacion,
                   COALESCE(m.origen, 'general') AS origen
            FROM movimientos m
            WHERE m.actividad_id = ? AND m.tipo = 'gasto'
            ORDER BY m.fecha DESC, m.id DESC
            """
            , (actividad_id,)
        )
        return render_template('actividad_detail.html', actividad=actividad, resumen=resumen, ingresos=ingresos, gastos=gastos)

    @app.route('/actividades/nueva', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def actividades_new():
        db = get_db()
        if request.method == 'POST':
            nombre = request.form.get('nombre', '').strip()
            fecha = request.form.get('fecha', '').strip()
            descripcion = request.form.get('descripcion', '').strip()
            try:
                validar_fecha(fecha)
            except Exception:
                flash('Fecha inválida.', 'danger')
                return render_template('actividades_form.html', actividad=None)
            db.execute('INSERT INTO actividades (nombre, fecha, descripcion) VALUES (?, ?, ?)', (nombre, fecha, descripcion))
            db.commit()
            flash('Actividad creada.', 'success')
            return redirect(url_for('actividades_list'))
        return render_template('actividades_form.html', actividad=None)

    @app.route('/actividades/<int:actividad_id>/editar', methods=['GET', 'POST'])
    @role_required('admin', 'tesorero')
    def actividades_edit(actividad_id: int):
        db = get_db()
        actividad = db.fetchone('SELECT * FROM actividades WHERE id=?', (actividad_id,))
        if not actividad:
            flash('Actividad no encontrada.', 'danger')
            return redirect(url_for('actividades_list'))
        if request.method == 'POST':
            nombre = request.form.get('nombre', '').strip()
            fecha = request.form.get('fecha', '').strip()
            descripcion = request.form.get('descripcion', '').strip()
            try:
                validar_fecha(fecha)
            except Exception:
                flash('Fecha inválida.', 'danger')
                return render_template('actividades_form.html', actividad=actividad)
            db.execute('UPDATE actividades SET nombre=?, fecha=?, descripcion=? WHERE id=?', (nombre, fecha, descripcion, actividad_id))
            db.commit()
            flash('Actividad actualizada.', 'success')
            return redirect(url_for('actividades_list'))
        return render_template('actividades_form.html', actividad=actividad)

    @app.post('/actividades/<int:actividad_id>/eliminar')
    @role_required('admin', 'tesorero')
    def actividades_delete(actividad_id: int):
        db = get_db()
        actividad = db.fetchone('SELECT nombre FROM actividades WHERE id=?', (actividad_id,))
        if not actividad:
            flash('Actividad no encontrada.', 'danger')
            return redirect(url_for('actividades_list'))
        db.execute('UPDATE movimientos SET actividad_id = NULL WHERE actividad_id = ?', (actividad_id,))
        db.execute('DELETE FROM actividades WHERE id = ?', (actividad_id,))
        db.commit()
        flash(f'Actividad eliminada: {actividad["nombre"]}.', 'success')
        return redirect(url_for('actividades_list'))

    @app.route('/cuotas')
    @login_required
    def cuotas_view():
        db = get_db()
        mes = request.args.get('mes') or datetime.today().strftime('%Y-%m')
        filas = resumen_cuotas_por_alumno(db, mes)
        total_esperado = sum(float(x['cuota_mensual']) for x in filas if x['activo'])
        total_pagado = sum(float(x['pagado']) for x in filas)
        total_debe = sum(max(float(x['cuota_mensual']) - float(x['pagado']), 0) for x in filas if x['activo'])
        alertas = obtener_alertas_morosidad(db, mes)
        return render_template('cuotas.html', filas=filas, mes=mes, total_esperado=total_esperado, total_pagado=total_pagado, total_debe=total_debe, alertas=alertas)

    @app.errorhandler(500)
    def internal_server_error(exc):
        app.logger.exception('Internal server error: %s', exc)
        flash('Ocurrió un error interno. Revisa los filtros de búsqueda e inténtalo nuevamente.', 'danger')
        destino = request.referrer or (url_for('dashboard') if current_user.is_authenticated else url_for('login'))
        return redirect(destino)

    return app


def is_postgres_url(url: str) -> bool:
    return url.startswith('postgresql://') or url.startswith('postgres://')


def sql_like_ci(value: str) -> str:
    return f"%{(value or '').strip().lower()}%"


def obtener_movimientos_filtrados(db: DBAdapter, tipo: str = 'Todos', mes: str = '', q: str = '', fecha_desde: str = '', fecha_hasta: str = '', actividad_id: str | int = '', alumno_id: str | int = ''):
    sql = """
        SELECT m.id, m.fecha, m.tipo, m.concepto, m.monto, COALESCE(a.nombre, '-') AS actividad,
               COALESCE(al.nombre, '-') AS alumno,
               COALESCE(m.origen, 'general') AS origen, COALESCE(m.observacion, '') AS observacion
        FROM movimientos m
        LEFT JOIN actividades a ON a.id = m.actividad_id
        LEFT JOIN alumnos al ON al.id = m.alumno_id
        WHERE 1=1
    """
    params: list[Any] = []
    if tipo in ('ingreso', 'gasto'):
        sql += ' AND m.tipo = ?'
        params.append(tipo)
    if mes:
        sql += ' AND substr(m.fecha, 1, 7) = ?'
        params.append(mes)
    if fecha_desde:
        sql += ' AND m.fecha >= ?'
        params.append(fecha_desde)
    if fecha_hasta:
        sql += ' AND m.fecha <= ?'
        params.append(fecha_hasta)
    if actividad_id:
        sql += ' AND m.actividad_id = ?'
        params.append(int(actividad_id))
    if alumno_id:
        sql += ' AND m.alumno_id = ?'
        params.append(int(alumno_id))
    if q:
        like = sql_like_ci(q)
        sql += " AND (LOWER(COALESCE(m.concepto, '')) LIKE ? OR LOWER(COALESCE(m.observacion, '')) LIKE ? OR LOWER(COALESCE(m.fecha, '')) LIKE ? OR LOWER(COALESCE(m.origen, '')) LIKE ? OR LOWER(COALESCE(a.nombre, '')) LIKE ? OR LOWER(COALESCE(al.nombre, '')) LIKE ? OR LOWER(COALESCE(al.curso, '')) LIKE ?)"
        params.extend([like, like, like, like, like, like, like])
    sql += ' ORDER BY m.fecha DESC, m.id DESC'
    return db.fetchall(sql, params)


def exportar_movimientos_pdf(movimientos, school_name: str, school_location: str, filtros: dict[str, str]) -> BytesIO:
    data = BytesIO()
    doc = SimpleDocTemplate(data, pagesize=landscape(A4), leftMargin=10*mm, rightMargin=10*mm, topMargin=10*mm, bottomMargin=10*mm)
    styles = getSampleStyleSheet()
    elems = []
    elems.append(Paragraph(f'{school_name} · {school_location}', styles['Title']))
    elems.append(Paragraph(f'Reporte de movimientos · generado {datetime.now().strftime("%Y-%m-%d %H:%M")}', styles['Normal']))
    filtros_txt = ' · '.join([f'{k}: {v}' for k, v in filtros.items()])
    elems.append(Paragraph(filtros_txt, styles['Normal']))
    elems.append(Spacer(1, 6))
    total_ing = sum(float(r['monto']) for r in movimientos if r['tipo'] == 'ingreso')
    total_gas = sum(float(r['monto']) for r in movimientos if r['tipo'] == 'gasto')
    elems.append(Paragraph(f'Registros: {len(movimientos)} · Ingresos: {formato_monto(total_ing)} · Gastos: {formato_monto(total_gas)} · Balance: {formato_monto(total_ing-total_gas)}', styles['Heading3']))
    table_data = [['Fecha', 'Tipo', 'Concepto', 'Actividad', 'Alumno', 'Origen', 'Monto']]
    for row in movimientos:
        concepto = str(row['concepto'])
        if len(concepto) > 38:
            concepto = concepto[:35] + '...'
        table_data.append([row['fecha'], row['tipo'], concepto, row['actividad'], row.get('alumno', '-'), row['origen'], formato_monto(row['monto'])])
    table = Table(table_data, repeatRows=1, colWidths=[24*mm, 20*mm, 78*mm, 42*mm, 42*mm, 30*mm, 24*mm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#dfe7ff')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#cbd5e1')),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (-1,1), (-1,-1), 'RIGHT'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f8fafc')]),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,0), 6),
        ('TOPPADDING', (0,0), (-1,0), 6),
    ]))
    elems.append(table)
    doc.build(elems)
    data.seek(0)
    return data


def crear_backup_db(database_path: str | Path) -> Path:
    database_path = str(database_path)
    BACKUP_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if is_postgres_url(database_path):
        destino = BACKUP_DIR / f'backup_contabilidad_{timestamp}.sql'
        parsed = urlparse(database_path)
        env = os.environ.copy()
        if parsed.password:
            env['PGPASSWORD'] = parsed.password
        cmd = [
            'pg_dump',
            '-h', parsed.hostname or 'localhost',
            '-p', str(parsed.port or 5432),
            '-U', parsed.username or 'postgres',
            '-d', (parsed.path or '/').lstrip('/'),
            '-f', str(destino),
        ]
        try:
            subprocess.run(cmd, check=True, env=env, capture_output=True)
        except FileNotFoundError as exc:
            raise RuntimeError('pg_dump no está instalado o no está en el PATH.') from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(exc.stderr.decode('utf-8', errors='ignore') or 'pg_dump falló.') from exc
        return destino
    origen = Path(database_path)
    destino = BACKUP_DIR / f'backup_contabilidad_{timestamp}.db'
    shutil.copy2(origen, destino)
    return destino


def listar_backups():
    if not BACKUP_DIR.exists():
        return []
    archivos = sorted([*BACKUP_DIR.glob('*.db'), *BACKUP_DIR.glob('*.sql')], key=lambda p: p.stat().st_mtime, reverse=True)
    return [
        {
            'nombre': p.name,
            'tamano': p.stat().st_size,
            'modificado': datetime.fromtimestamp(p.stat().st_mtime),
        }
        for p in archivos
    ]


def formato_monto(valor: Any) -> str:
    try:
        valor = float(valor or 0)
    except Exception:
        valor = 0
    return f"${valor:,.0f}".replace(',', '.')


def parse_float(raw: str) -> float:
    txt = (raw or '').strip()
    if not txt:
        return 0.0
    txt = txt.replace('.', '').replace(',', '.') if ',' in txt else txt.replace(',', '.')
    return float(txt)


def validar_fecha(fecha: str) -> bool:
    datetime.strptime(fecha, '%Y-%m-%d')
    return True


def estado_cuota(cuota: Any, pagado: Any) -> tuple[str, str]:
    cuota_f = float(cuota or 0)
    pagado_f = float(pagado or 0)
    if cuota_f <= 0:
        return ('Sin cuota', '⚪')
    if pagado_f >= cuota_f:
        return ('Pagado', '🟢')
    if pagado_f > 0:
        return ('Parcial', '🟡')
    return ('Deuda', '🔴')


def obtener_nombre_alumno(db: DBAdapter, alumno_id: int) -> str:
    row = db.fetchone('SELECT nombre FROM alumnos WHERE id=?', (alumno_id,))
    return row['nombre'] if row else 'Alumno'


def alumno_duplicado(db: DBAdapter, nombre: str, curso: str, exclude_id: int | None = None) -> bool:
    sql = "SELECT id FROM alumnos WHERE lower(trim(nombre)) = lower(trim(?)) AND lower(trim(COALESCE(curso, ''))) = lower(trim(?))"
    params: list[Any] = [nombre, curso or '']
    if exclude_id:
        sql += ' AND id <> ?'
        params.append(exclude_id)
    return db.fetchone(sql, params) is not None


def pago_duplicado(db: DBAdapter, alumno_id: int, mes: str) -> bool:
    row = db.fetchone('SELECT id FROM pagos_alumnos WHERE alumno_id = ? AND mes = ?', (alumno_id, mes))
    return row is not None


def registrar_pago_alumno(db: DBAdapter, alumno_id: int, fecha: str, mes: str, monto: float, observacion: str, actividad_id: int | None = None, tipo_pago: str = 'cuota_mensual') -> None:
    alumno = db.fetchone('SELECT * FROM alumnos WHERE id = ?', (alumno_id,))
    if not alumno:
        raise ValueError('Alumno no encontrado')
    if tipo_pago == 'cuota_mensual':
        concepto = f'Cuota mensual alumno: {alumno["nombre"]} ({mes})'
        if db.kind == 'postgres':
            cur = db.execute(
                'INSERT INTO movimientos (fecha, tipo, concepto, monto, actividad_id, alumno_id, observacion, origen) VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id',
                (fecha, 'ingreso', concepto, monto, None, alumno_id, observacion, 'cuota_mensual'),
            )
            movimiento_id = cur.fetchone()['id']
        else:
            cur = db.execute(
                'INSERT INTO movimientos (fecha, tipo, concepto, monto, actividad_id, alumno_id, observacion, origen) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (fecha, 'ingreso', concepto, monto, None, alumno_id, observacion, 'cuota_mensual'),
            )
            movimiento_id = cur.lastrowid
        db.execute(
            'INSERT INTO pagos_alumnos (alumno_id, fecha, mes, monto, observacion, movimiento_id) VALUES (?, ?, ?, ?, ?, ?)',
            (alumno_id, fecha, mes, monto, observacion, movimiento_id),
        )
    else:
        concepto = f'Aporte actividad alumno: {alumno["nombre"]}'
        detalle = observacion if observacion else f'Aporte para actividad registrado en {mes}'
        db.execute(
            'INSERT INTO movimientos (fecha, tipo, concepto, monto, actividad_id, alumno_id, observacion, origen) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (fecha, 'ingreso', concepto, monto, actividad_id, alumno_id, detalle, 'actividad_alumno'),
        )


def resumen_cuotas_por_alumno(db: DBAdapter, mes: str):
    return db.fetchall(
        """
        SELECT a.id, a.nombre, a.curso, a.cuota_mensual, a.activo,
               COALESCE(SUM(CASE WHEN p.mes = ? THEN p.monto ELSE 0 END), 0) AS pagado
        FROM alumnos a
        LEFT JOIN pagos_alumnos p ON a.id = p.alumno_id
        GROUP BY a.id, a.nombre, a.curso, a.cuota_mensual, a.activo
        ORDER BY a.nombre
        """,
        (mes,),
    )


def obtener_alertas_morosidad(db: DBAdapter, mes: str):
    alertas = []
    for fila in resumen_cuotas_por_alumno(db, mes):
        if not fila['activo']:
            continue
        debe = max(float(fila['cuota_mensual']) - float(fila['pagado']), 0)
        if debe > 0:
            estado, icono = estado_cuota(fila['cuota_mensual'], fila['pagado'])
            alertas.append({
                'alumno_id': fila['id'],
                'nombre': fila['nombre'],
                'curso': fila['curso'],
                'debe': debe,
                'estado': estado,
                'icono': icono,
            })
    return alertas


def seed_default_admin(db: DBAdapter) -> None:
    has_user = db.fetchone('SELECT 1 FROM usuarios LIMIT 1')
    if has_user:
        return
    username = os.environ.get('ADMIN_USER', 'admin')
    password = os.environ.get('ADMIN_PASSWORD', 'admin123')
    nombre = os.environ.get('ADMIN_NAME', 'Administrador')
    db.execute(
        'INSERT INTO usuarios (username, password_hash, role, nombre, activo) VALUES (?, ?, ?, ?, 1)',
        (username, generate_password_hash(password), 'admin', nombre),
    )
    db.commit()


def init_db(db: DBAdapter) -> None:
    if db.kind == 'postgres':
        script = """
        CREATE TABLE IF NOT EXISTS actividades (
            id BIGSERIAL PRIMARY KEY,
            nombre TEXT NOT NULL,
            fecha TEXT,
            descripcion TEXT
        );

        CREATE TABLE IF NOT EXISTS movimientos (
            id BIGSERIAL PRIMARY KEY,
            fecha TEXT NOT NULL,
            tipo TEXT NOT NULL CHECK(tipo IN ('ingreso', 'gasto')),
            concepto TEXT NOT NULL,
            monto DOUBLE PRECISION NOT NULL CHECK(monto >= 0),
            actividad_id BIGINT,
            alumno_id BIGINT,
            observacion TEXT,
            origen TEXT NOT NULL DEFAULT 'general',
            CONSTRAINT fk_mov_actividad FOREIGN KEY (actividad_id) REFERENCES actividades(id)
        );

        CREATE TABLE IF NOT EXISTS alumnos (
            id BIGSERIAL PRIMARY KEY,
            nombre TEXT NOT NULL,
            curso TEXT,
            cuota_mensual DOUBLE PRECISION NOT NULL DEFAULT 0,
            activo INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS pagos_alumnos (
            id BIGSERIAL PRIMARY KEY,
            alumno_id BIGINT NOT NULL,
            fecha TEXT NOT NULL,
            mes TEXT NOT NULL,
            monto DOUBLE PRECISION NOT NULL CHECK(monto >= 0),
            observacion TEXT,
            movimiento_id BIGINT,
            CONSTRAINT fk_pagos_alumno FOREIGN KEY (alumno_id) REFERENCES alumnos(id),
            CONSTRAINT fk_pagos_mov FOREIGN KEY (movimiento_id) REFERENCES movimientos(id)
        );

        CREATE TABLE IF NOT EXISTS usuarios (
            id BIGSERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin', 'tesorero', 'solo_lectura')),
            nombre TEXT NOT NULL,
            activo INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_movimientos_fecha ON movimientos(fecha);
        CREATE INDEX IF NOT EXISTS idx_pagos_alumnos_mes ON pagos_alumnos(mes);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alumnos_nombre_curso_unique ON alumnos ((lower(trim(nombre))), (lower(trim(COALESCE(curso, '')))));
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pagos_alumno_mes_unique ON pagos_alumnos(alumno_id, mes);
        """
    else:
        script = """
        CREATE TABLE IF NOT EXISTS actividades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            fecha TEXT,
            descripcion TEXT
        );

        CREATE TABLE IF NOT EXISTS movimientos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            tipo TEXT NOT NULL CHECK(tipo IN ('ingreso', 'gasto')),
            concepto TEXT NOT NULL,
            monto REAL NOT NULL CHECK(monto >= 0),
            actividad_id INTEGER,
            alumno_id INTEGER,
            observacion TEXT,
            origen TEXT NOT NULL DEFAULT 'general',
            FOREIGN KEY (actividad_id) REFERENCES actividades(id)
        );

        CREATE TABLE IF NOT EXISTS alumnos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            curso TEXT,
            cuota_mensual REAL NOT NULL DEFAULT 0,
            activo INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS pagos_alumnos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alumno_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            mes TEXT NOT NULL,
            monto REAL NOT NULL CHECK(monto >= 0),
            observacion TEXT,
            movimiento_id INTEGER,
            FOREIGN KEY (alumno_id) REFERENCES alumnos(id),
            FOREIGN KEY (movimiento_id) REFERENCES movimientos(id)
        );

        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin', 'tesorero', 'solo_lectura')),
            nombre TEXT NOT NULL,
            activo INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_movimientos_fecha ON movimientos(fecha);
        CREATE INDEX IF NOT EXISTS idx_pagos_alumnos_mes ON pagos_alumnos(mes);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alumnos_nombre_curso_unique ON alumnos(lower(trim(nombre)), lower(trim(COALESCE(curso, ''))));
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pagos_alumno_mes_unique ON pagos_alumnos(alumno_id, mes);
        """
    db.executescript(script)

    # Migraciones suaves para bases existentes creadas con versiones anteriores.
    for statement in [
        'ALTER TABLE actividades ADD COLUMN descripcion TEXT',
        'ALTER TABLE movimientos ADD COLUMN actividad_id BIGINT',
        'ALTER TABLE movimientos ADD COLUMN alumno_id BIGINT',
        "ALTER TABLE movimientos ADD COLUMN origen TEXT NOT NULL DEFAULT 'general'",
        'ALTER TABLE pagos_alumnos ADD COLUMN observacion TEXT',
        'ALTER TABLE pagos_alumnos ADD COLUMN movimiento_id BIGINT',
    ]:
        try:
            db.execute(statement)
        except Exception:
            pass
    db.commit()


app = create_app()

if __name__ == '__main__':
    host = os.environ.get('APP_HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', os.environ.get('APP_PORT', '10000')))
    debug = os.environ.get('APP_DEBUG', '0') == '1'
    app.run(host=host, port=port, debug=debug, use_reloader=False)
