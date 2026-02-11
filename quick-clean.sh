#!/bin/bash
# One-command Git history cleaner for main/master branch
# This script requires NO interaction - but will ask for confirmation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}   Quick Git History Cleaner${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Detect the default branch name
DEFAULT_BRANCH=""
if git show-ref --verify --quiet refs/heads/main; then
    DEFAULT_BRANCH="main"
elif git show-ref --verify --quiet refs/heads/master; then
    DEFAULT_BRANCH="master"
else
    echo -e "${RED}❌ Error: Could not detect main or master branch${NC}"
    exit 1
fi

echo -e "Detected default branch: ${GREEN}$DEFAULT_BRANCH${NC}"
echo ""
echo -e "${RED}⚠️  WARNING: This will PERMANENTLY delete all Git history!${NC}"
echo -e "${RED}⚠️  The repository will be reduced to a single 'Initial commit'${NC}"
echo ""
echo "Current commit count: $(git rev-list --count HEAD)"
echo ""
read -p "Type 'yes' to proceed with cleaning $DEFAULT_BRANCH: " confirmation

if [ "$confirmation" != "yes" ]; then
    echo "Operation cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}Starting cleanup...${NC}"

# Create orphan branch
echo "→ Creating orphan branch..."
if ! git checkout --orphan temp-clean-history-$$ 2>&1 | grep -v "Switched to"; then
    echo -e "${RED}❌ Failed to create orphan branch${NC}"
    exit 1
fi

# Stage all files
echo "→ Staging all files..."
if ! git add -A 2>&1; then
    echo -e "${RED}❌ Failed to stage files${NC}"
    exit 1
fi

# Create initial commit
echo "→ Creating initial commit..."
if ! git commit -m "Initial commit" 2>&1 | grep -v "create mode"; then
    echo -e "${RED}❌ Failed to create commit${NC}"
    echo "Tip: Ensure git user.name and user.email are configured:"
    echo "  git config user.name \"Your Name\""
    echo "  git config user.email \"your@email.com\""
    exit 1
fi

# Verify single commit
COMMIT_COUNT=$(git rev-list --count HEAD)
if [ "$COMMIT_COUNT" != "1" ]; then
    echo -e "${RED}❌ Error: Expected 1 commit, found $COMMIT_COUNT${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Single commit verified${NC}"

# Replace the main/master branch
echo "→ Replacing $DEFAULT_BRANCH branch..."
git branch -D "$DEFAULT_BRANCH" 2>/dev/null || true
git branch -m "$DEFAULT_BRANCH"

echo ""
echo -e "${GREEN}✅ Local cleanup complete!${NC}"
echo ""
echo "New commit count: $(git rev-list --count HEAD)"
echo ""
echo -e "${YELLOW}Final step: Force push to remote${NC}"
echo -e "${RED}⚠️  This is IRREVERSIBLE!${NC}"
echo ""
read -p "Force push to origin/$DEFAULT_BRANCH? (type 'yes' to proceed): " push_confirmation

if [ "$push_confirmation" != "yes" ]; then
    echo ""
    echo -e "${YELLOW}Push cancelled. Local branch cleaned.${NC}"
    echo "To push later, run:"
    echo -e "  ${GREEN}git push -f origin $DEFAULT_BRANCH${NC}"
    exit 0
fi

echo ""
echo "→ Force pushing to origin/$DEFAULT_BRANCH..."
if git push -f origin "$DEFAULT_BRANCH"; then
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}   ✅ Git history successfully cleaned!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "Repository now has a single initial commit:"
    git log --oneline
    echo ""
    echo -e "${YELLOW}⚠️  IMPORTANT: Notify all collaborators to re-clone!${NC}"
    echo ""
    echo "They should run:"
    echo -e "  ${GREEN}git fetch origin${NC}"
    echo -e "  ${GREEN}git reset --hard origin/$DEFAULT_BRANCH${NC}"
    echo ""
else
    echo -e "${RED}❌ Push failed${NC}"
    echo ""
    echo "Possible reasons:"
    echo "  - Branch protection is enabled (disable in GitHub settings)"
    echo "  - Insufficient permissions"
    echo "  - Network issue"
    echo ""
    echo "Your local branch has been cleaned. To retry push later:"
    echo -e "  ${GREEN}git push -f origin $DEFAULT_BRANCH${NC}"
    exit 1
fi
