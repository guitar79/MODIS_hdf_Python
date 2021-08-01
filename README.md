
# MODIS_hdf_Python
anaconda environment

conda create -n MODIS_hdf_Python_env python=3.8
conda env list

# activate 가상환경 시작
mac/linux
source activate MODIS_hdf_Python_env

windows
activate MODIS_hdf_Python_env

# deactivate 가상환경 종료
mac/linux
conda deactivate

windows
deactivate

# install module
conda install pandas
conda install -c conda-forge pyhdf basemap basemap-date-hires


# 가상환경 내보내기 (export)
conda env export > MODIS_hdf_Python_env.yaml

# .yaml 파일로 새로운 가상환경 만들기
conda env create -f MODIS_hdf_python_env.yaml

# 가상환경 리스트 출력
conda env list

# 가상환경 제거하기
conda env remove -n MODIS_hdf_Python_env  
