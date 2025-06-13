# Pin to amd64 because Dinamica only gets distributed for that platform
FROM ghcr.io/mmyrte/evoland:latest
LABEL authors="Carlson Büth, Jan Hartman" \
  version="8.3" \
  description="Docker image for Dinamica EGO."

# Vignettes are being built with quarto
RUN /rocker_scripts/install_quarto.sh

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
