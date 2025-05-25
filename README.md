# Sistema de Archivos Distribuidos por Bloques (DFS)

## Objetivo

Diseñar e implementar un sistema de archivos distribuidos por bloques minimalista, inspirado en sistemas como GFS (Google File System) y HDFS (Hadoop Distributed File System).

## Descripción

Un sistema de archivos distribuido (DFS) permite compartir y acceder de forma concurrente a archivos almacenados en diferentes nodos. Existen dos enfoques principales para el diseño de un DFS:

- **Basado en bloques:**  
  - La unidad de lectura y escritura es el bloque, que puede estar distribuido en diferentes nodos.
  - El sistema operativo cliente garantiza transparencia, permitiendo acceder archivos locales y remotos de la misma forma (ej: NFS, AFS, SMB).

- **Basado en objetos:**  
  - La unidad de distribución es el archivo completo, no el bloque.
  - No permite actualización parcial de archivos, solo reemplazo completo (WORM: Write Once, Read Many).
  - No se integra directamente con el sistema operativo, sino mediante SDK o API.

Para este proyecto, se propone un DFS intermedio: basado en bloques, pero con la característica WORM de los sistemas de almacenamiento por objetos, similar a GFS y HDFS.

## Arquitectura

El sistema está compuesto por los siguientes elementos:

### 1. Clientes

- Acceden al sistema mediante:
  - **CLI (Interfaz de Línea de Comandos):** Para operaciones manuales o administrativas.
  - **API/SDK:** Para integración programática.
- Representan múltiples usuarios o aplicaciones.

### 2. NameNodes (Gestión de Metadatos)

- **Leader:**  
  - Nodo maestro principal.
  - Gestiona el espacio de nombres (metadatos, estructura de archivos/directorios).
  - Coordina operaciones de lectura/escritura.
- **Follower:**  
  - Réplica del Leader para alta disponibilidad.
  - Puede asumir el rol de Leader en caso de fallo (usando protocolos de consenso como Raft o ZooKeeper).

### 3. DataNodes (Almacenamiento Distribuido)

- Almacenan los bloques de datos reales.
- Replican datos entre sí para redundancia y tolerancia a fallos.
- Siguen instrucciones del NameNode Leader para replicación y balanceo de datos.

### Flujo de Operaciones

1. El cliente solicita metadatos (ubicación de archivos, permisos, etc.) al NameNode Leader.
2. El NameNode Leader responde con la ubicación de los DataNodes relevantes.
3. El cliente lee o escribe datos directamente en los DataNodes.
4. Los DataNodes sincronizan los bloques de datos entre sí para mantener la consistencia.

## Especificaciones Técnicas

- **Protocolos de comunicación:**  
  - Se deben emplear REST API y gRPC.
  - Canal de control y canal de datos diferenciados.

- **Gestión de archivos:**
  - Lectura y escritura directa entre Cliente y DataNode.
  - Algoritmo para distribución y replicación de bloques.
  - Cada archivo se divide en n bloques distribuidos entre los DataNodes.
  - Replicación mínima: cada bloque debe estar en al menos dos DataNodes.
  - El NameNode entrega al cliente la lista y orden de los bloques y sus ubicaciones (URIs).

- **Escritura de archivos:**
  - El cliente transfiere bloques directamente a los DataNodes seleccionados por el NameNode.
  - El DataNode que recibe un bloque se convierte en "Leader" de ese bloque y se encarga de replicarlo a un "Follower".

- **Interfaz de comandos (CLI):**
  - Debe implementar los siguientes comandos básicos:  
    `ls`, `cd`, `put`, `get`, `mkdir`, `rmdir`, `rm`, etc.

- **(Opcional) Autenticación:**
  - Implementar autenticación básica (usuario/contraseña) para que cada cliente solo vea y manipule sus propios archivos.

## Recomendaciones

- Leer y comprender los papers fundacionales de GFS y HDFS:
  - [Google File System (Wikipedia)](https://es.wikipedia.org/wiki/Google_File_System)
  - [The Google File System (Paper)](https://g.co/kgs/XzwmU76)
  - [Hadoop Distributed File System (Wikipedia)](https://es.wikipedia.org/wiki/Hadoop_Distributed_File_System) 