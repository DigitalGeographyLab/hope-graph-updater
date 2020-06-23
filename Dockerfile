FROM continuumio/miniconda3

ADD src /src
WORKDIR /src/aqi_updater
RUN conda env create -f /src/conda-env.yml && conda clean -afy
ENV PATH /opt/conda/envs/aqi-env/bin:$PATH

RUN chmod +x start-application.sh
CMD ./start-application.sh