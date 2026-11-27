#![allow(clippy::cognitive_complexity)]

use crate::rp_common::{
    get_provider_metadata, init_log, issuer_url, register_client, CERTIFICATION_BASE_URL, RP_NAME,
};

use log::{debug, info};

mod rp_common;

#[test]
#[ignore]
fn rp_discovery_openid_configuration() {
    const TEST_ID: &str = "rp-discovery-openid-configuration";
    init_log(TEST_ID);

    let _issuer_url = issuer_url(TEST_ID);
    let provider_metadata = get_provider_metadata(TEST_ID);

    macro_rules! log_field {
        ($field:ident) => {
            log_container_field!(provider_metadata.$field);
        };
    }

    log_info!(
        "Successfully retrieved provider metadata from {:?}",
        _issuer_url
    );
    log_field!(issuer);
    log_field!(authorization_endpoint);
    log_field!(token_endpoint);
    log_field!(userinfo_endpoint);
    log_field!(jwks_uri);
    log_field!(registration_endpoint);
    log_field!(scopes_supported);
    log_field!(response_types_supported);
    log_field!(response_modes_supported);
    log_field!(grant_types_supported);
    log_field!(acr_values_supported);
    log_field!(subject_types_supported);
    log_field!(id_token_signing_alg_values_supported);
    log_field!(id_token_encryption_alg_values_supported);
    log_field!(id_token_encryption_enc_values_supported);
    log_field!(userinfo_signing_alg_values_supported);
    log_field!(userinfo_encryption_alg_values_supported);
    log_field!(userinfo_encryption_enc_values_supported);
    log_field!(request_object_signing_alg_values_supported);
    log_field!(request_object_encryption_alg_values_supported);
    log_field!(request_object_encryption_enc_values_supported);
    log_field!(token_endpoint_auth_methods_supported);
    log_field!(token_endpoint_auth_signing_alg_values_supported);
    log_field!(display_values_supported);
    log_field!(claim_types_supported);
    log_field!(claims_supported);
    log_field!(service_documentation);
    log_field!(claims_locales_supported);
    log_field!(ui_locales_supported);
    log_field!(claims_parameter_supported);
    log_field!(request_parameter_supported);
    log_field!(request_uri_parameter_supported);
    log_field!(require_request_uri_registration);
    log_field!(op_policy_uri);
    log_field!(op_tos_uri);

    log_debug!("Provider metadata: {:?}", provider_metadata);

    log_info!("SUCCESS");
}

#[test]
#[ignore]
fn rp_registration_dynamic() {
    const TEST_ID: &str = "rp-registration-dynamic";
    init_log(TEST_ID);

    let _issuer_url = issuer_url(TEST_ID);
    let provider_metadata = get_provider_metadata(TEST_ID);
    let registration_response = register_client(&provider_metadata, |reg| reg);

    macro_rules! log_field {
        ($field:ident) => {
            log_container_field!(registration_response.$field);
        };
    }

    log_field!(client_id);
    log_field!(client_secret);
    log_field!(registration_access_token);
    log_field!(registration_client_uri);
    log_field!(client_id_issued_at);
    log_field!(client_secret_expires_at);
    log_field!(redirect_uris);
    log_field!(response_types);
    log_field!(grant_types);
    log_field!(application_type);
    log_field!(contacts);
    log_field!(client_name);
    log_field!(logo_uri);
    log_field!(client_uri);
    log_field!(policy_uri);
    log_field!(tos_uri);
    log_field!(jwks_uri);
    log_field!(jwks);
    log_field!(sector_identifier_uri);
    log_field!(subject_type);
    log_field!(id_token_signed_response_alg);
    log_field!(id_token_encrypted_response_alg);
    log_field!(id_token_encrypted_response_enc);
    log_field!(userinfo_signed_response_alg);
    log_field!(userinfo_encrypted_response_alg);
    log_field!(userinfo_encrypted_response_enc);
    log_field!(request_object_signing_alg);
    log_field!(request_object_encryption_alg);
    log_field!(request_object_encryption_enc);
    log_field!(token_endpoint_auth_method);
    log_field!(token_endpoint_auth_signing_alg);
    log_field!(default_max_age);
    log_field!(require_auth_time);
    log_field!(default_acr_values);
    log_field!(initiate_login_uri);
    log_field!(request_uris);

    log_debug!("Registration response: {:?}", registration_response);

    assert_eq!(
        format!(
            "{}/{}/registration?client_id={}",
            CERTIFICATION_BASE_URL,
            RP_NAME,
            **registration_response.client_id()
        ),
        registration_response
            .registration_client_uri()
            .unwrap()
            .to_string()
    );

    log_info!("SUCCESS");
}
