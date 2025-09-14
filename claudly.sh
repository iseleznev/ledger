# Create target folder
#mkdir -p /path/to/target_folder

# Copy all .txt and .pdf files from current directory recursively
mkdir -p claudly
#find . -type f \( -name "*.java" -o -name "pom.xml" -o -name "*.sql" -o -name "*.proto" \) -exec cp {} claudly/ \;
find . -type f \( -name "*.java" -o -name "*.sql" \) -exec cp {} claudly/ \;