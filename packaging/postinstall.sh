#!/bin/sh
set -e

# Dedicated system user.
if ! getent passwd piko >/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin piko
fi

# Log directory.
mkdir -p /var/log/piko
chown piko:piko /var/log/piko
chmod 0750 /var/log/piko

# Default configuration (config.yaml + conf.d/woocommerce.yaml); --init
# refuses to overwrite existing files.
if [ ! -f /etc/piko/config.yaml ]; then
    /usr/bin/piko --init -config /etc/piko/config.yaml
fi
chown -R piko:piko /etc/piko
chmod 0700 /etc/piko
chmod 0600 /etc/piko/config.yaml

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload || true
fi

echo "piko installed. Edit /etc/piko/config.yaml, then: systemctl enable --now piko"
