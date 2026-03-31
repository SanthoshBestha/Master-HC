"""
app.py  –  OSS Master HC Portal (Flask + MySQL)
Run:  python app.py
"""
from flask import (Flask, render_template, request, jsonify,
                   session, redirect, url_for, flash)
from functools import wraps
from config import SECRET_KEY
from database import db_manager

app = Flask(__name__)
app.secret_key = SECRET_KEY


# ── Auth decorators ───────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if session.get('user_type') not in roles:
                return jsonify({'error': 'Unauthorized'}), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return redirect(url_for('dashboard') if 'user_id' in session else url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        user = db_manager.get_user_by_credentials(username, password)
        if user:
            session.update({
                'user_id':   user['id'],
                'username':  user['username'],
                'name':      user['name'],
                'user_type': user['user_type'],
                'ref_id':    user['ref_id'],
            })
            return redirect(url_for('dashboard'))
        flash('Invalid username or password', 'danger')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    t = session.get('user_type')
    tmpl = {'admin': 'admin.html', 'approver': 'approver.html'}.get(t, 'user.html')
    return render_template(tmpl, user=session)


# ── API: HC Data ──────────────────────────────────────────────────────────────

@app.route('/api/hc')
@login_required
def get_hc():
    data = db_manager.get_hc(
        region    = request.args.get('region', ''),
        hub       = request.args.get('hub', ''),
        search    = request.args.get('search', ''),
        user_type = session['user_type'],
        ref_id    = session['ref_id'],
    )
    return jsonify([dict(r) for r in data])


@app.route('/api/filters')
@login_required
def get_filters():
    return jsonify({
        'regions': db_manager.get_distinct('region'),
        'hubs':    db_manager.get_distinct('hub'),
    })


# ── API: Admin – Edit HC ──────────────────────────────────────────────────────

@app.route('/api/admin/hc/<int:site_id>', methods=['PUT'])
@login_required
@role_required('admin')
def edit_hc(site_id):
    approved_count = request.json.get('approved_count')
    if approved_count is None:
        return jsonify({'error': 'approved_count required'}), 400
    db_manager.update_hc(site_id, approved_count, session['username'])
    return jsonify({'success': True})


@app.route('/api/admin/changelog')
@login_required
@role_required('admin')
def get_changelog():
    rows = db_manager.get_changelog(search=request.args.get('search', ''))
    return jsonify([dict(r) for r in rows])


# ── API: User Management ──────────────────────────────────────────────────────

@app.route('/api/admin/users', methods=['GET'])
@login_required
@role_required('admin')
def get_users():
    return jsonify([dict(r) for r in db_manager.get_all_users()])


@app.route('/api/admin/users', methods=['POST'])
@login_required
@role_required('admin')
def create_user():
    data = request.json
    if not all(k in data for k in ['name', 'username', 'password', 'user_type']):
        return jsonify({'error': 'Missing fields'}), 400
    if data['user_type'] in ('user', 'approver') and not data.get('ref_id'):
        return jsonify({'error': 'ref_id required for this user type'}), 400
    try:
        db_manager.create_user(data)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/admin/users/<int:uid>', methods=['DELETE'])
@login_required
@role_required('admin')
def delete_user(uid):
    db_manager.delete_user(uid)
    return jsonify({'success': True})


@app.route('/api/admin/user_ids')
@login_required
@role_required('admin')
def get_user_ids():
    return jsonify({
        'user_ids':     db_manager.get_distinct_user_ids(),
        'approver_ids': db_manager.get_distinct_approver_ids(),
    })


# ── API: Requests ─────────────────────────────────────────────────────────────

@app.route('/api/requests', methods=['GET'])
@login_required
def get_requests():
    rows = db_manager.get_requests(
        user_type = session['user_type'],
        ref_id    = session['ref_id'],
        search    = request.args.get('search', ''),
        view      = request.args.get('view', 'all'),
        username  = session['username'],
    )
    return jsonify([dict(r) for r in rows])


@app.route('/api/requests', methods=['POST'])
@login_required
def create_request():
    data = request.json
    if not all(k in data for k in ['site_id', 'change_type', 'new_count', 'justification']):
        return jsonify({'error': 'Missing fields'}), 400
    if data['change_type'] == 'decrease' and not data.get('decrease_type'):
        return jsonify({'error': 'decrease_type required'}), 400
    try:
        db_manager.create_request(data, session['username'], session['user_type'])
        return jsonify({'success': True})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/requests/<int:req_id>/status', methods=['PUT'])
@login_required
def update_status(req_id):
    if session['user_type'] == 'user':
        return jsonify({'error': 'Unauthorized'}), 403
    # Check if already Processed – nobody can change it
    req = db_manager.get_request_by_id(req_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    if req['status'] == 'Processed':
        return jsonify({'error': 'This request has been Processed and cannot be changed'}), 400
    new_status = request.json.get('status')
    allowed = ['New', 'Pending from Approver', 'Ready to Process']
    if new_status not in allowed:
        return jsonify({'error': 'Invalid status'}), 400
    if session['user_type'] == 'approver':
        if req['approver_id'] != session['ref_id']:
            return jsonify({'error': 'Unauthorized'}), 403
    db_manager.update_request_status(req_id, new_status, session['username'])
    return jsonify({'success': True})


@app.route('/api/requests/<int:req_id>/process', methods=['PUT'])
@login_required
@role_required('admin')
def mark_processed(req_id):
    """Mark as Processed. Admin only. Irreversible – no one can change it after."""
    req = db_manager.get_request_by_id(req_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    if req['status'] == 'Processed':
        return jsonify({'error': 'Already marked as Processed'}), 400
    comment = request.json.get('comment', '').strip()
    db_manager.mark_request_processed(req_id, session['username'], comment)
    return jsonify({'success': True})


@app.route('/api/requests/<int:req_id>/approve', methods=['PUT'])
@login_required
@role_required('approver')
def approve_request(req_id):
    req = db_manager.get_request_by_id(req_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    if req['status'] == 'Processed':
        return jsonify({'error': 'This request has been Processed and cannot be changed'}), 400
    if req['approver_id'] != session['ref_id']:
        return jsonify({'error': 'Unauthorized'}), 403
    approved = request.json.get('approved', True)
    comment  = request.json.get('comment', '')
    status   = 'Ready to Process' if approved else 'Pending from Approver'
    db_manager.update_request_status(
        req_id, status, session['username'],
        comment=comment, approved=approved
    )
    return jsonify({'success': True})


@app.route('/api/sites')
@login_required
def get_sites():
    rows = db_manager.get_sites_for_request(
        user_type = session['user_type'],
        ref_id    = session['ref_id'],
    )
    return jsonify([dict(r) for r in rows])


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("Initialising MySQL database …")
    db_manager.init_db()
    print("Database ready.  Starting server on http://localhost:5000")
    app.run(debug=True, port=5000)
