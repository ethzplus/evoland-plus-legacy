# Pin to amd64 because Dinamica only gets distributed for that platform
FROM --platform=amd64 rocker/geospatial:4.5
LABEL authors="Carlson Büth, Jan Hartman" \
  version="8.3" \
  description="Docker image for Dinamica EGO."

ENV DINAMICA_TARGET_DIR="/opt/dinamica"
ENV CRAN="https://cloud.r-project.org/"
ENV DINAMICA_EGO_DOWNLOAD_URL="https://dinamicaego.com/nui_download/1960/"

# Install Dinamica and its R integration.
# The script downloads the AppImage if it's not found; choosing to do copy this from the
# build system because the download is unstable from Europe and may need to be repeated
# upon image corruption.
COPY .devcontainer/DinamicaEGO-830-Ubuntu-LTS.AppImage "${DINAMICA_TARGET_DIR}/DinamicaEGO.AppImage"
COPY .devcontainer/install_dinamica.sh /opt/dinamica/install_dinamica.sh
RUN /opt/dinamica/install_dinamica.sh

# Hand-knitted dependency discovery from DESCRIPTION avoids cache invalidation versus a
# remotes::install_deps based solution.
WORKDIR /builddir
COPY DESCRIPTION /builddir/DESCRIPTION
COPY .devcontainer/install_pkg_deps.r /builddir/install_pkg_deps.r
RUN /builddir/install_pkg_deps.r && rm -r /builddir
WORKDIR /

# Install development dependencies for vscode-style development.
COPY .devcontainer/install_vscode_devtools.sh /rocker_scripts/install_vscode_devtools.sh
RUN /rocker_scripts/install_vscode_devtools.sh
