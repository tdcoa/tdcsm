from tdcsm.flask_server import app
from flask import make_response, Blueprint, redirect, url_for, jsonify, render_template, request, session
from . import setup


@app.route('/', methods=['POST', 'GET'])
@app.route('/home', methods=['POST', 'GET'])
@app.route('/initial', methods=['POST', 'GET'])
def initial_setup():
    if request.method == 'POST':
        if request.json['post_name'] == 'FS_SETUP':
            path = request.json['path']

            # copy default secrets & config files to provided path
            setup.set_folder(path)

            session['path'] = path

            return jsonify({})

        elif request.json['post_name'] == 'TEST_CONNECTION':
            system_info = request.json['system_info']

            # test connection using passed in system info
            conn_status, error_msg = setup.test_connection(system_info)

            # conn_status = True
            # error_msg = ''
            return jsonify({'conn_status': conn_status,
                            'error_msg': error_msg})

        elif request.json['post_name'] == 'SAVE_SOURCE_SYSTEMS':
            source_systems = request.json['source_systems']

            setup.save_source_system(source_systems, session['path'])

            return jsonify({})

    elif request.method == 'GET':
        return render_template('initial_setup.html')
