let source_systems = {};
let target_system = {};

// -- DOCUMENT READY FUNCTION ----------------------------------
$(document).ready( function() {
    // activate all dropdowns
    $('.ui.dropdown').dropdown();

    // STEP 1: show next button once folder setup path entered
    $('#folder_browser1').on('focusout',function(){
        if ($(this).val().length > 0) {
            $('#next_button1').show();
        }
    });

    // Transition to target system config
    $('#next_button2').on('click', function(){
        $('#step_2').removeClass('active').addClass('completed');
        $('#step_3').addClass('active');

        $('#step_2_body').transition({
            animation: 'slide right',
            onComplete: function(){
                $('#step_3_body').transition('slide left');
                $('#target_system_1')
                    .form('set values', {
                        system_name: 'Trancend',
                        host: 'tdprdcop3.td.teradata.com',
                        username: 'nd186026',
                        password: 'Bugmug8878!',
                        logmech: 'ldap',
                        db_coa: 'adlste_coa',
                        db_region: 'adlste_westcomm'
                    });
            }
        })
    });

    // DEBUG ONLY
    $('#folder_browser1').val('C:\\Users\\nd186026\\Documents\\tdcsm_web_demo');
    $('#next_button1').show().click();
    $('#source_system_1')
        .form('set values', {
            system_name: 'Trancend_Target',
            site_id: 'trancend_target_01',
            host: 'tdprdcop3.td.teradata.com',
            username: 'nd186026',
            password: 'Bugmug8878!',
            logmech: 'ldap',
            environment: 'dev'
        });
    $('#next_button2').click();
});
// -------------------------------------------------------------


// -- FORMAT SOURCE SYSTEM -------------------------------------
function format_source_system(sys_info){

    if (sys_info['logmech'] === 'regular'){
        sys_info['logmech'] = ''
    }

    let formatted_sys = {};
    formatted_sys[sys_info['system_name']] = {
        'siteid': sys_info['site_id'],
        'active': 'true',
        'host': sys_info['host'],
        'username': sys_info['username'],
        'password': sys_info['password'],
        'logmech': sys_info['logmech'],
        'use': sys_info['environment']
    };

    return formatted_sys

}
// ------------------------------------------------------------


// -- 1. FOLDER SETUP - SET PATH ------------------------------
function folder_setup_set_path(element){
    $(element).addClass('loading');

    let data = JSON.stringify({
        post_name: "FS_SETUP",
        path: $('#folder_browser1').val().trim()
    });

    $.ajax({
        url: "/initial",
        type: "POST",
        cache: false,
        contentType: 'application/json',
        data: data,
        dataType: 'json',
        success: function (response) {
            console.log('Secrets.yaml & config.yaml set');
            $(element).removeClass('loading');

            // transition to next step
            $('#step_1').removeClass('active').addClass('completed');
            $('#step_2').addClass('active');

            $('#step_1_body').transition({
                animation: 'slide right',
                onComplete: function(){
                    $('#step_2_body').transition('slide left');
                }
            })
        },

        error: function (response) {
            console.log(response);
            $(element).removeClass('loading');
        }
    });

}
// ------------------------------------------------------------


// -- TEST CONNECTION -----------------------------------------
function test_connection(element, system_type){
    let $parent_form = $(element).closest('form');
    let system_info = $parent_form.form('get values');

    $(element).addClass('loading');
    $parent_form.removeClass('success error');

    let data = JSON.stringify({
        post_name: "TEST_CONNECTION",
        system_info: system_info,
        system_type: system_type
    });

    $.ajax({
        url: "/initial",
        type: "POST",
        cache: false,
        contentType: 'application/json',
        data: data,
        dataType: 'json',
        success: function (response) {
            console.log('Connection test complete');
            $(element).removeClass('loading');

            // successful connection
            if (response.conn_status){
                $parent_form.addClass('success');

                // save source system
                if (system_type === 'source') {
                    save_source_systems(system_info, $parent_form);
                    $('#next_button2').show();
                }

            }

            // failed connection
            else {
                console.log(response.error_msg);
                $parent_form.addClass('error');
                $parent_form.find('.error.message').find('p').html(response.error_msg);  // set error message
            }
        },

        error: function (response) {
            console.log(response);
            $(element).removeClass('loading');
        }
    });
}
// ------------------------------------------------------------


// -- SAVE SOURCE SYSTEM --------------------------------------
function save_source_systems(system, $parent_form){

    system = format_source_system(system);
    source_systems[Object.keys(system)] = Object.values(system)[0];
    // console.log(source_systems);

    let data = JSON.stringify({
        post_name: "SAVE_SOURCE_SYSTEMS",
        source_systems: source_systems
    });

    $.ajax({
        url: "/initial",
        type: "POST",
        cache: false,
        contentType: 'application/json',
        data: data,
        dataType: 'json',
        success: function (response) {
            console.log('Source systems updated');
            $parent_form.find('.success.message').find('p').append(' System configuration saved.');  // set error message
        },

        error: function (response) {
            console.log(response);
        }
    })
}
// ------------------------------------------------------------