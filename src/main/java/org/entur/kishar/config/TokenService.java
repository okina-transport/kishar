package org.entur.kishar.config;

import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TokenService {
    private static final Logger log = LoggerFactory.getLogger(TokenService.class);
    private static final String BEARER_PREFIX = "Bearer ";

    private final Keycloak keycloakClient;

    public TokenService(@Value("${iam.keycloak.admin.client}") String clientId,
                        @Value("${iam.keycloak.client.secret}") String clientSecret,
                        @Value("${keycloak.realm}") String realm,
                        @Value("${keycloak.auth-server-url}") String authServerUrl) {
        log.debug("Build keycloak client with url {} and realm {}", authServerUrl, realm);
        this.keycloakClient = KeycloakBuilder.builder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .realm(realm)
                .serverUrl(authServerUrl)
                .grantType("client_credentials")
                .build();
    }

    public String getAuthorizationHeader() {
        log.debug("Get authorization header from keycloak");
        return BEARER_PREFIX + getToken();
    }

    private String getToken() {
        try {
            return keycloakClient.tokenManager().getAccessTokenString();
        } catch (Exception e){
            log.error("Error while getting token", e);
            return "emptyToken";
        }
    }

}
