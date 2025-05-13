#!/usr/bin/env bash

# Abort script if any command exits with non-zero status. Not foolproof.
set -e 

# Init default variables; this script should, however, be called from a Dockerfile
DINAMICA_TARGET_DIR=${DINAMICA_TARGET_DIR:-"/opt/dinamica"}
DINAMICA_EGO_DOWNLOAD_URL=${DINAMICA_EGO_DOWNLOAD_URL:-"https://dinamicaego.com/nui_download/1960/"}
CRAN=${CRAN:-"https://cloud.r-project.org/"} # posit mirror proxy for optimal selection
LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-""}
original_dir=$(pwd)

# Setup of target dir
mkdir -p "$DINAMICA_TARGET_DIR"

# Dinamica is distributed for Linux as .AppImage for x86 only
# The AppImage contains a squashfs with system dependencies which is usually mounted
# upon GUI startup; we extract the files to a persistent directory
# ($DINAMICA_TARGET_DIR). The `DinamicaConsole` and other binaries are in
# `squashfs-root/usr/bin`, the bundled system libraries in `squashfs-root/usr/lib`.
# TODO remove redundant system dependencies
if [ ! -f "$DINAMICA_TARGET_DIR/DinamicaEGO.AppImage" ]; then
  # only download if file's not there
  wget --quiet "$DINAMICA_EGO_DOWNLOAD_URL" -O "$DINAMICA_TARGET_DIR/DinamicaEGO.AppImage"
fi
chmod +x "$DINAMICA_TARGET_DIR/DinamicaEGO.AppImage"

# need to change dir, because appimage-extract does not have option for target
cd "$DINAMICA_TARGET_DIR" && \
  "$DINAMICA_TARGET_DIR/DinamicaEGO.AppImage" --appimage-extract && \
  cd "$original_dir"

# Find and install dinamica R package
dinamica_rpkg=$(find "$DINAMICA_TARGET_DIR/squashfs-root/usr/bin/Data/R/" -name 'Dinamica_*.tar.gz')
R -e "remotes::install_local('$dinamica_rpkg')"

# Write default config, later exported as $REGISTRY_FILE
cat << EOF > "$DINAMICA_TARGET_DIR/.dinamica_ego_8.conf"
AlternativePathForR = "/usr/bin/Rscript"
ClConfig = "0"
MemoryAllocationPolicy = "1"
RCranMirror = "$CRAN"
EOF

# Persist environment variables.
# Assuming debian/ubuntu canonical location for exporting variables in a shell-agnostic manner
cat << EOF > /etc/profile.d/dinamica.sh
#!/usr/bin/env bash
export LD_LIBRARY_PATH=$DINAMICA_TARGET_DIR/squashfs-root/usr/lib/:$LD_LIBRARY_PATH
export DINAMICA_EGO_8_INSTALLATION_DIRECTORY=$DINAMICA_TARGET_DIR/squashfs-root/usr/bin
export REGISTRY_FILE=$DINAMICA_TARGET_DIR/.dinamica_ego_8.conf
export PATH=$PATH:$DINAMICA_TARGET_DIR/squashfs-root/usr/bin
EOF

echo "PATH=$PATH:$DINAMICA_TARGET_DIR/squashfs-root/usr/bin" >>"${R_HOME}/etc/Renviron.site"

# Cleanup
rm "$DINAMICA_TARGET_DIR/DinamicaEGO.AppImage"
rm -rf /tmp/downloaded_packages
