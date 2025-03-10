package org.entur.kishar.config;

import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TokenService {
    private static Keycloak keycloakClient;
    private static final Logger log = LoggerFactory.getLogger(TokenService.class);

    @Value("${iam.keycloak.admin.client}")
    private String clientId;

    @Value("${iam.keycloak.client.secret}")
    private String clientSecret;

    @Value("${keycloak.realm}")
    private String realm;

    @Value("${keycloak.auth-server-url}")
    private String authServerUrl;

    private TokenService() {
        log.info("Token service initialized");
        log.info("clientId:" + clientId);
        log.info("realm:" + realm);
        log.info("authServerUrl:" + authServerUrl);

    }

    public String getToken() {

        if (keycloakClient == null ){

            keycloakClient = KeycloakBuilder.builder().clientId(clientId).clientSecret(clientSecret).realm(realm).serverUrl(authServerUrl).grantType("client_credentials").build();
        }

        return keycloakClient.tokenManager().getAccessTokenString();
    }

}
