FROM nginx:1.30.6-alpine@sha256:c59d1f8b1e0e42da75be973424cd72e23f7f8adfb54e982f4d1cd0216a5a5576

# Official swagger-ui-dist 5.34.0 package from registry.npmjs.org.
# ADD verifies the exact artifact before it enters the image.
ADD --checksum=sha256:15cdd55151558cf29ee70fb485332251a9911ebe27414b243ddd10fef9942394 https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-5.34.0.tgz /tmp/swagger-ui-dist-5.34.0.tgz

RUN tar -xzf /tmp/swagger-ui-dist-5.34.0.tgz -C /usr/share/nginx/html --strip-components=1 \
        package/LICENSE package/NOTICE package/swagger-ui.css \
        package/swagger-ui-bundle.js package/swagger-ui-standalone-preset.js \
        package/favicon-32x32.png \
    && rm /tmp/swagger-ui-dist-5.34.0.tgz

COPY nginx.conf /etc/nginx/nginx.conf
COPY index.html /usr/share/nginx/html/index.html
COPY swagger-config.js /usr/share/nginx/html/swagger-config.js
COPY specs/ /usr/share/nginx/html/specs/

EXPOSE 8080
USER 101:101
ENTRYPOINT ["nginx"]
CMD ["-g", "daemon off;"]
