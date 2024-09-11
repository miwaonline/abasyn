#!/bin/bash
ABASYN_PKG_VERSION=3.0-1
# Create necessary directories
mkdir -p debian
mkdir -p opt/abasyn/{abasyn,etc}
mkdir -p usr/lib/systemd/system

# Place your files into the respective directories
cp ../abasyn/*py opt/abasyn/abasyn/
rm opt/abasyn/abasyn/win.py
cp ../etc/* opt/abasyn/etc/

# Create the abasyn.service file
echo "[Unit]
Description=Abasyn Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/abasyn/abasyn/app.py
User=nobody
Group=nogroup
Environment=PYTHONUNBUFFERED=1
WorkingDirectory=/opt/abasyn
RestartSec=10
Restart=on-failure

[Install]
WantedBy=multi-user.target" > usr/lib/systemd/system/abasyn.service

# Create debian control files
echo "Source: abasyn
Section: misc
Priority: optional
Maintainer: Mykhailo Masyk <mykhailo.masyk@gmail.com>
Standards-Version: 4.1.5
Build-Depends: debhelper (>= 11)

Package: abasyn
Architecture: any
Depends: \${shlibs:Depends}, \${misc:Depends}, python3-simplejson, python3-flask, systemd, python3-yaml, python3-waitress, python3-fdb, libfbclient2
Description: Abasyn package
 This package installs Abasyn software." > debian/control

echo "opt/abasyn
opt/abasyn/abasyn
opt/abasyn/etc
usr/lib/systemd/system" > debian/dirs

echo "opt/abasyn/abasyn/* opt/abasyn/abasyn/
opt/abasyn/etc/* opt/abasyn/etc/
usr/lib/systemd/system/abasyn.service usr/lib/systemd/system/" > debian/install

echo "/opt/abasyn/etc/abasyn.yml" > debian/conffiles

# Create the postinst script
echo "#!/bin/bash
set -e

systemctl daemon-reload
systemctl enable abasyn.service
systemctl start abasyn.service" > debian/postinst
chmod 755 debian/postinst

# Create the prerm script
echo "#!/bin/bash
set -e

systemctl stop abasyn.service
systemctl disable abasyn.service" > debian/prerm
chmod 755 debian/prerm

# Create the changelog file
echo "abasyn ($abasyn_PKG_VERSION) unstable; urgency=low

  * Completely rewritten release.

 -- Mykhailo Masyk <Mykhailo.Masyk@gmail.com>  Tue, 09 Jul 2024 00:00:00 +0000" > debian/changelog

# Create the rules file
echo "#!/usr/bin/make -f

%:
	dh \$@" > debian/rules
chmod 755 debian/rules

# Create the compat file
echo "12" > debian/compat

# Build the package
dpkg-buildpackage -us -uc

# Clean up
rm -rf debian/
rm -rf opt/
rm -rf usr/
rm -f ../abasyn_${abasyn_PKG_VERSION}_amd64.buildinfo
rm -f ../abasyn_${abasyn_PKG_VERSION}_amd64.changes
rm -f ../abasyn_${abasyn_PKG_VERSION}.dsc
rm -f ../abasyn_${abasyn_PKG_VERSION}.tar.gz
rm -f ../abasyn-dbgsym_${abasyn_PKG_VERSION}_amd64.ddeb
# Test the package; uncomment once ready
# piuparts abasyn.deb
